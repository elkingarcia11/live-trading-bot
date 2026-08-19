"""Capture complete Schwab option-chain snapshots during regular hours."""

from __future__ import annotations

import csv
import io
import logging
import threading
from datetime import date, datetime, time as day_time
from pathlib import Path
from typing import Any, Optional, Sequence
from zoneinfo import ZoneInfo

from option_selector import underlying_price_from_chain
from schwab_options_chain_client import SchwabOptionsChainClient

logger = logging.getLogger(__name__)

CHAIN_COLUMNS = (
    "timestamp",
    "mark_price",
    "underlying_price",
)


class OptionChainRecorder:
    """Poll and persist every call and put contract in the configured chain."""

    def __init__(
        self,
        chain_client: SchwabOptionsChainClient,
        *,
        symbols: Sequence[str],
        local_root: str | Path,
        timezone_name: str = "America/New_York",
        interval_seconds: float = 15.0,
        days_to_expiration: Optional[int] = None,
        strike_count: Optional[int] = None,
    ) -> None:
        if not symbols:
            raise ValueError("At least one option-chain symbol is required")
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self._chain_client = chain_client
        self._symbols = tuple(str(symbol).upper() for symbol in symbols)
        self._local_root = Path(local_root)
        self._timezone = ZoneInfo(timezone_name)
        self._interval_seconds = interval_seconds
        self._days_to_expiration = days_to_expiration
        self._strike_count = strike_count
        self._pending: dict[tuple[str, date], list[dict[str, str]]] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @property
    def buffered_row_count(self) -> int:
        with self._lock:
            return sum(len(rows) for rows in self._pending.values())

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="option-chain-recorder",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "Option-chain capture enabled for %s (%ss, regular hours)",
            ", ".join(self._symbols),
            self._interval_seconds,
        )

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None and threading.current_thread() is not self._thread:
            self._thread.join(timeout=max(self._interval_seconds, 5.0) + 2.0)
        self.flush()
        self._thread = None

    def flush(self) -> int:
        """Merge buffered snapshots into strike/date-partitioned CSV files."""
        with self._lock:
            pending = self._pending
            self._pending = {}
        written = 0
        try:
            for (side, session_date), rows in sorted(pending.items()):
                if not rows:
                    continue
                for strike, strike_rows in _group_by_strike(rows):
                    path = (
                        self._local_root
                        / side
                        / session_date.isoformat()
                        / f"{strike}.csv"
                    )
                    written += self._merge_csv(path, strike_rows)
        except Exception:
            with self._lock:
                for key, rows in pending.items():
                    self._pending.setdefault(key, []).extend(rows)
            raise
        if written:
            logger.info(
                "Option-chain flush: wrote %d new contract snapshot(s)", written)
        return written

    def poll_once(self, *, now: Optional[datetime] = None) -> int:
        """Fetch all configured chains once when ``now`` is in regular hours."""
        current = now or datetime.now(self._timezone)
        local = current.astimezone(self._timezone)
        if not _is_regular_hours(local):
            return 0
        timestamp = local.isoformat()
        captured = 0
        for underlying in self._symbols:
            try:
                chain = self._chain_client.fetch_chain(
                    underlying,
                    contract_type="ALL",
                    strike_count=self._strike_count,
                    days_to_expiration=self._days_to_expiration,
                    include_underlying_quote=True,
                )
                rows = _flatten_chain(
                    chain,
                    underlying_price=underlying_price_from_chain(chain),
                    timestamp=timestamp,
                )
                with self._lock:
                    for side, side_rows in rows.items():
                        self._pending.setdefault(
                            (side, local.date()), []).extend(side_rows)
                captured += sum(len(side_rows) for side_rows in rows.values())
            except Exception:
                logger.exception(
                    "Option-chain capture failed for %s", underlying)
        return captured

    def _run_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.poll_once()
                if self.buffered_row_count:
                    self.flush()
            except Exception:
                logger.exception(
                    "Option-chain capture loop failed; will retry")
            self._stop.wait(self._interval_seconds)

    @staticmethod
    def _merge_csv(path: Path, incoming: list[dict[str, str]]) -> int:
        path.parent.mkdir(parents=True, exist_ok=True)
        existing: list[dict[str, str]] = []
        if path.exists():
            with path.open("r", newline="", encoding="utf-8") as handle:
                existing = list(csv.DictReader(handle))
        seen = {
            row.get("timestamp", "")
            for row in existing
        }
        new_rows = []
        for row in incoming:
            key = row["timestamp"]
            if key in seen:
                continue
            seen.add(key)
            new_rows.append(row)
        if not new_rows:
            return 0
        all_rows = existing + new_rows
        payload = io.StringIO(newline="")
        writer = csv.DictWriter(payload, fieldnames=CHAIN_COLUMNS)
        writer.writeheader()
        writer.writerows(
            {column: row.get(column, "") for column in CHAIN_COLUMNS}
            for row in all_rows
        )
        temporary = path.with_suffix(".csv.tmp")
        temporary.write_text(payload.getvalue(), encoding="utf-8")
        temporary.replace(path)
        return len(new_rows)


def _is_regular_hours(timestamp: datetime) -> bool:
    current = timestamp.timetz().replace(tzinfo=None)
    return day_time(9, 30) <= current <= day_time(16, 0)


def _flatten_chain(
    chain: dict[str, Any], *, underlying_price: Optional[float], timestamp: str
) -> dict[str, list[dict[str, str]]]:
    flattened: dict[str, list[dict[str, str]]] = {"calls": [], "puts": []}
    for map_key, side in (("callExpDateMap", "calls"), ("putExpDateMap", "puts")):
        expiration_map = chain.get(map_key)
        if not isinstance(expiration_map, dict):
            continue
        for expiration, strikes in expiration_map.items():
            if not isinstance(strikes, dict):
                continue
            for strike, contracts in strikes.items():
                if not isinstance(contracts, list):
                    continue
                for contract in contracts:
                    if not isinstance(contract, dict):
                        continue
                    flattened[side].append(_contract_row(
                        contract,
                        underlying_price=underlying_price,
                        timestamp=timestamp,
                        strike=strike,
                    ))
    return flattened


def _contract_row(
    contract: dict[str, Any], *, underlying_price: Optional[float],
    timestamp: str, strike: str,
) -> dict[str, str]:
    return {
        "timestamp": timestamp,
        "mark_price": _csv_value(contract.get("mark")),
        "underlying_price": _csv_value(underlying_price),
        "_strike": strike,
    }


def _group_by_strike(
    rows: list[dict[str, str]],
) -> list[tuple[str, list[dict[str, str]]]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        strike = _strike_filename(row.get("_strike", ""))
        grouped.setdefault(strike, []).append(row)
    return sorted(grouped.items())


def _strike_filename(value: str) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value).strip() or "unknown"
    return f"{numeric:.8f}".rstrip("0").rstrip(".")


def _csv_value(value: Any) -> str:
    return "" if value is None else str(value)
