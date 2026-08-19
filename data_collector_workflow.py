"""Raw Databento futures and Schwab ATM-options collection workflow."""

from __future__ import annotations

import csv
import io
import logging
import os
import signal
import smtplib
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, time as day_time, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional
from zoneinfo import ZoneInfo

from google.cloud import storage

from option_selector import select_atm_call_from_chain, select_atm_put_from_chain
from session_trade_recorder import databento_trade_row
from schwab_streamer import SchwabStreamSession, build_schwab_stream_processor

logger = logging.getLogger(__name__)

OPTION_COLUMNS = ("timestamp", "description", "mark_price", "underlying_price")


class DailyCsvBuffer:
    """Thread-safe daily CSV buffer with local durability and GCS replication."""

    def __init__(
        self,
        *,
        local_root: str | Path,
        bucket_name: str,
        client: Optional[storage.Client] = None,
        remote_enabled: bool = True,
        prefix: str = "",
        filename: str,
        columns: Iterable[str],
        key_columns: Iterable[str],
    ) -> None:
        self._local_root = Path(local_root)
        self._bucket_name = bucket_name
        self._client = client
        self._bucket = None
        self._remote_enabled = remote_enabled
        self._prefix = prefix.strip("/")
        self._filename = filename
        self._columns = tuple(columns)
        self._key_columns = tuple(key_columns)
        self._pending: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._write_lock = threading.Lock()

    @property
    def buffered_count(self) -> int:
        with self._lock:
            return len(self._pending)

    def add(self, row: Mapping[str, Any]) -> None:
        with self._lock:
            self._pending.append({column: row.get(column, "")
                                 for column in self._columns})

    def flush(self) -> int:
        with self._write_lock:
            with self._lock:
                rows = self._pending
                self._pending = []
            if not rows:
                return 0
            by_day: dict[str, list[dict[str, Any]]] = {}
            for row in rows:
                day = _date_key(row.get("timestamp"))
                by_day.setdefault(day, []).append(row)
            try:
                written = 0
                for day, day_rows in sorted(by_day.items()):
                    written += self._flush_day(day, day_rows)
                return written
            except Exception:
                with self._lock:
                    self._pending = rows + self._pending
                raise

    def _flush_day(self, day: str, incoming: list[dict[str, Any]]) -> int:
        path = self._local_path(day)
        path.parent.mkdir(parents=True, exist_ok=True)
        existing = _read_csv(path, self._columns)
        seen = {_row_key(row, self._key_columns) for row in existing}
        merged = list(existing)
        for row in incoming:
            key = _row_key(row, self._key_columns)
            if key in seen:
                continue
            seen.add(key)
            merged.append(row)
        payload = _csv_bytes(merged, self._columns)
        temp = path.with_suffix(".csv.tmp")
        temp.write_bytes(payload)
        temp.replace(path)
        if self._remote_enabled:
            blob = self._ensure_bucket().blob(self._remote_path(day))
            blob.upload_from_file(io.BytesIO(payload), content_type="text/csv")
        return len(merged) - len(existing)

    def _local_path(self, day: str) -> Path:
        return self._local_root / day / self._filename

    def _remote_path(self, day: str) -> str:
        return "/".join(part for part in (self._prefix, day, self._filename) if part)

    def _ensure_bucket(self):
        if self._bucket is None:
            self._bucket = (self._client or storage.Client()
                            ).bucket(self._bucket_name)
        return self._bucket


class ConnectionEmailer:
    """Best-effort connection state notifications; email failure never stops capture."""

    def __init__(self, app: Any) -> None:
        email = app.email
        self._enabled = bool(email.forward_test and email.recipients)
        self._host = email.smtp_host
        self._port = int(email.smtp_port)
        self._sender = email.sender
        self._password = _secret("GMAIL_APP_PASSWORD")
        self._recipients = tuple(email.recipients)

    def notify(self, component: str, connected: bool, detail: str = "") -> None:
        if not self._enabled or not self._password:
            return
        state = "connected" if connected else "disconnected"
        message = EmailMessage()
        message["Subject"] = f"Data collector {component} {state}"
        message["From"] = self._sender
        message["To"] = ", ".join(self._recipients)
        message.set_content(f"{component} is {state}. {detail}".strip())
        try:
            with smtplib.SMTP(self._host, self._port, timeout=20) as smtp:
                smtp.starttls()
                smtp.login(self._sender, self._password)
                smtp.send_message(message)
        except Exception:
            logger.exception(
                "Could not send %s connection notification", component)


class SchwabAtmOptionsCollector:
    """Observe and rotate the ATM SPY call and put during regular hours."""

    def __init__(self, app: Any, *, on_state: Callable[[str, bool, str], None]) -> None:
        from schwab_options_chain_client import SchwabOptionsChainClient

        self._app = app
        self._chain = SchwabOptionsChainClient.from_config(app)
        self._on_state = on_state
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._update_thread: Optional[threading.Thread] = None
        self._session: Optional[SchwabStreamSession] = None
        self._active: dict[str, str] = {}
        self._pending_underlying: Optional[float] = None
        gcs = app.gcs
        client = storage.Client()
        self._call = DailyCsvBuffer(
            local_root=Path(gcs.local_fallback_path) / "collector",
            bucket_name=gcs.bucket_name,
            client=client,
            filename="call.csv",
            columns=OPTION_COLUMNS,
            key_columns=OPTION_COLUMNS,
        )
        self._put = DailyCsvBuffer(
            local_root=Path(gcs.local_fallback_path) / "collector",
            bucket_name=gcs.bucket_name,
            client=client,
            filename="put.csv",
            columns=OPTION_COLUMNS,
            key_columns=OPTION_COLUMNS,
        )

    def start(self) -> None:
        processor = build_schwab_stream_processor(
            symbols=("SPY",), consumers=[])
        self._session = SchwabStreamSession.from_env(
            symbols=("SPY",),
            processor=processor,
            on_open_external=lambda: self._on_state(
                "Schwab options", True, "stream open"),
            on_close_external=lambda code, reason: self._on_state(
                "Schwab options", False, f"close={code} {reason or ''}"
            ),
            on_error_external=lambda error: logger.warning(
                "Schwab stream error: %s", error),
            on_option_quote=self._on_quote,
        )
        self._stop.clear()
        self._update_thread = threading.Thread(
            target=self._update_loop, name="atm-option-selector", daemon=True)
        self._update_thread.start()
        self._session.connect()
        try:
            chain = self._chain.fetch_chain(
                "SPY",
                contract_type="ALL",
                strike_count=self._app.options.strike_count,
                days_to_expiration=self._app.options.days_to_expiration,
                include_underlying_quote=True,
            )
            underlying = _chain_underlying_price(chain)
            if underlying is not None:
                self._select_contracts(underlying)
        except Exception:
            logger.exception("Could not seed initial ATM SPY options")

    def stop(self) -> None:
        self._stop.set()
        if self._session is not None:
            self._session.disconnect()
        if self._update_thread is not None:
            self._update_thread.join(timeout=5)
        for buffer in (self._call, self._put):
            try:
                buffer.flush()
            except Exception:
                logger.exception("Final Schwab options flush failed")

    def _on_quote(self, quote: dict[str, Any]) -> None:
        symbol = str(quote.get("symbol") or "").upper()
        underlying = _positive_float(quote.get("underlying_price"))
        if underlying is not None:
            with self._lock:
                self._pending_underlying = underlying
        with self._lock:
            side = next(
                (name for name, active in self._active.items() if active == symbol), None)
        if side is None or underlying is None:
            return
        timestamp = _quote_timestamp(quote.get("quote_time"))
        row = {
            "timestamp": timestamp,
            "description": str(quote.get("description") or symbol),
            "mark_price": float(quote["mark"]),
            "underlying_price": underlying,
        }
        (self._call if side == "call" else self._put).add(row)

    def _update_loop(self) -> None:
        last_flush = time.monotonic()
        while not self._stop.wait(0.25):
            with self._lock:
                underlying = self._pending_underlying
                self._pending_underlying = None
            if underlying is not None:
                try:
                    self._select_contracts(underlying)
                except Exception:
                    logger.exception("Could not resolve ATM SPY options")
            if time.monotonic() - last_flush >= 15:
                for buffer in (self._call, self._put):
                    try:
                        buffer.flush()
                    except Exception:
                        logger.exception(
                            "Periodic Schwab options flush failed")
                last_flush = time.monotonic()

    def _select_contracts(self, underlying: float) -> None:
        chain = self._chain.fetch_chain(
            "SPY",
            contract_type="ALL",
            strike_count=self._app.options.strike_count,
            days_to_expiration=self._app.options.days_to_expiration,
            include_underlying_quote=True,
        )
        as_of = datetime.now(ZoneInfo(self._app.app.timezone)).date()
        call = select_atm_call_from_chain(
            chain, "SPY", underlying, target_dte=self._app.options.days_to_expiration, as_of=as_of
        )
        put = select_atm_put_from_chain(
            chain, "SPY", underlying, target_dte=self._app.options.days_to_expiration, as_of=as_of
        )
        desired = {"call": call.occ_symbol, "put": put.occ_symbol}
        with self._lock:
            current = dict(self._active)
        if desired == current or self._session is None:
            return
        for side, symbol in desired.items():
            if current.get(side) != symbol:
                self._session.subscribe_option(symbol)
        for side, symbol in current.items():
            if desired.get(side) != symbol:
                self._session.unsubscribe_option(symbol)
        with self._lock:
            self._active = desired


class DataCollectorWorkflow:
    """Run independent 24/7 ES and regular-hours SPY ATM option collectors."""

    def __init__(self, app: Any) -> None:
        self._app = app
        self._stop = threading.Event()
        self._emailer = ConnectionEmailer(app)
        gcs = app.gcs
        client = storage.Client()
        self._es = DailyCsvBuffer(
            local_root=Path(gcs.local_fallback_path) / "collector",
            bucket_name=gcs.bucket_name,
            client=client,
            filename="es.csv",
            columns=("timestamp", "symbol", "raw_symbol", "price", "price_raw", "size", "side", "action", "depth", "flags",
                     "sequence", "instrument_id", "publisher_id", "rtype", "ts_event", "ts_recv", "ts_in_delta", "ts_index"),
            key_columns=("ts_event", "sequence", "instrument_id"),
        )
        self._options = SchwabAtmOptionsCollector(
            app, on_state=self._emailer.notify)

    def run(self) -> None:
        self._install_signals()
        options_thread = threading.Thread(
            target=self._options_schedule, name="options-schedule", daemon=True)
        options_thread.start()
        self._run_databento()
        self._options.stop()

    def _run_databento(self) -> None:
        import databento as db

        from config import normalize_market_symbol, secret

        key = secret("DATABENTO_API_KEY") or self._app.databento.api_key
        symbols = self._app.market.stream_symbols or self._app.market.symbols
        dataset = self._app.databento.dataset
        stype_in = self._app.databento.stype_in or "raw_symbol"
        while not self._stop.is_set():
            client = db.Live(key=key.strip())
            symbol_by_id: dict[int, str] = {}
            raw_symbol_by_id: dict[int, str] = {}

            def on_record(record: Any) -> None:
                kind = type(record).__name__
                if kind == "SymbolMappingMsg":
                    instrument_id = int(
                        getattr(record, "instrument_id", 0) or 0)
                    mapped = normalize_market_symbol(str(getattr(
                        record, "stype_in_symbol", "") or getattr(record, "stype_out_symbol", "")))
                    symbol_by_id[instrument_id] = mapped
                    raw_symbol_by_id[instrument_id] = str(
                        getattr(record, "stype_out_symbol", "") or mapped)
                    return
                if kind != "TradeMsg":
                    return
                instrument_id = int(getattr(record, "instrument_id", 0) or 0)
                row = databento_trade_row(record, symbol=symbol_by_id.get(
                    instrument_id, symbols[0]), raw_symbol=raw_symbol_by_id.get(instrument_id, ""))
                if row is not None:
                    self._es.add(row)

            def on_error(error: Exception) -> None:
                logger.exception("Databento stream error: %s", error)
                self._emailer.notify("Databento ES", False, str(error))

            try:
                client.subscribe(dataset=dataset, schema="trades",
                                 symbols=list(symbols), stype_in=stype_in)
                client.add_callback(on_record, exception_callback=on_error)
                client.start()
                self._emailer.notify("Databento ES", True, "stream open")
                last_flush = time.monotonic()
                while not self._stop.wait(0.25):
                    if time.monotonic() - last_flush >= 15:
                        self._es.flush()
                        last_flush = time.monotonic()
            except Exception as error:
                logger.exception("Databento connection failed: %s", error)
                self._emailer.notify("Databento ES", False, str(error))
                self._stop.wait(1.0)
            finally:
                for method in ("stop", "terminate"):
                    try:
                        getattr(client, method)()
                    except Exception:
                        pass
        try:
            self._es.flush()
        except Exception:
            logger.exception("Final Databento flush failed")

    def _options_schedule(self) -> None:
        timezone_name = self._app.app.timezone
        tz = ZoneInfo(timezone_name)
        active = False
        while not self._stop.wait(1.0):
            now = datetime.now(tz)
            should_run = now.weekday() < 5 and day_time(
                9, 30) <= now.time() < day_time(16, 0)
            if should_run and not active:
                try:
                    self._options.start()
                    active = True
                except Exception:
                    logger.exception(
                        "Could not start Schwab options collector")
            elif not should_run and active:
                self._options.stop()
                active = False

    def _install_signals(self) -> None:
        def stop(*_: object) -> None:
            self._stop.set()

        signal.signal(signal.SIGINT, stop)
        signal.signal(signal.SIGTERM, stop)


def _read_csv(path: Path, columns: tuple[str, ...]) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle) if any(row.values())]


def _csv_bytes(rows: list[Mapping[str, Any]], columns: tuple[str, ...]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=columns)
    writer.writeheader()
    writer.writerows({column: row.get(column, "")
                     for column in columns} for row in rows)
    return output.getvalue().encode("utf-8")


def _row_key(row: Mapping[str, Any], columns: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(str(row.get(column, "")) for column in columns)


def _date_key(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).date().isoformat()
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date().isoformat()


def _quote_timestamp(value: Any) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    millis = int(float(value))
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc)


def _positive_float(value: Any) -> Optional[float]:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result > 0 else None


def _chain_underlying_price(chain: Mapping[str, Any]) -> Optional[float]:
    """Extract the current underlying price from a Schwab chain response."""
    quote = chain.get("underlying")
    if not isinstance(quote, Mapping):
        return None
    for key in ("last", "mark", "close", "underlyingPrice"):
        value = _positive_float(quote.get(key))
        if value is not None:
            return value
    return None


def _secret(name: str) -> str:
    try:
        from config import secret

        return secret(name)
    except Exception:
        return os.getenv(name, "")


def main() -> int:
    from schwab_auth import _load_dotenv
    from config import get_config

    _load_dotenv()
    DataCollectorWorkflow(get_config(reload=True)).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
