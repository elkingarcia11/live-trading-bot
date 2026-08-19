"""Raw Databento ES trade collection with periodic local and GCS flushes."""

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
from datetime import date, datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

from google.cloud import storage

from session_trade_recorder import databento_trade_row
from tick_bar_builder import TickBarBuilder

logger = logging.getLogger(__name__)

TICK_TIMEFRAMES = ("25t", "50t", "100t", "200t", "400t", "800t", "1200t")
TICK_BAR_COLUMNS = (
    "timestamp", "symbol", "timeframe", "open", "high", "low", "close",
    "volume", "end", "partial", "ticks",
)


class DailyCsvBuffer:
    """Thread-safe CSV buffer with local durability and GCS replication."""

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
            try:
                return self._flush_continuous(rows)
            except Exception:
                with self._lock:
                    self._pending = rows + self._pending
                raise

    def _flush_continuous(self, incoming: list[dict[str, Any]]) -> int:
        path = self._local_path()
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
            blob = self._ensure_bucket().blob(self._remote_path())
            blob.upload_from_file(io.BytesIO(payload), content_type="text/csv")
        return len(merged) - len(existing)

    def _local_path(self) -> Path:
        return self._local_root / self._filename

    def _remote_path(self) -> str:
        return "/".join(part for part in (self._prefix, self._filename) if part)

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


class DataCollectorWorkflow:
    """Run 24/7 ES trade capture and periodic GCS flushes."""

    def __init__(self, app: Any) -> None:
        self._app = app
        self._stop = threading.Event()
        self._emailer = ConnectionEmailer(app)
        gcs = app.gcs
        client = storage.Client()
        self._es = DailyCsvBuffer(
            local_root=Path(gcs.local_fallback_path) /
            "collector" / "continuous_data",
            bucket_name=gcs.bucket_name,
            client=client,
            prefix="continuous_data",
            filename="es.csv",
            columns=("timestamp", "symbol", "raw_symbol", "price", "price_raw", "size", "side", "action", "depth", "flags",
                     "sequence", "instrument_id", "publisher_id", "rtype", "ts_event", "ts_recv", "ts_in_delta", "ts_index"),
            key_columns=("ts_event", "sequence", "instrument_id"),
        )
        self._es_tick_builders = {
            timeframe: TickBarBuilder(
                ticks_per_bar=int(timeframe[:-1]))
            for timeframe in TICK_TIMEFRAMES
        }
        self._es_tick_buffers = {
            timeframe: DailyCsvBuffer(
                local_root=Path(gcs.local_fallback_path) /
                "collector" / "continuous_data",
                bucket_name=gcs.bucket_name,
                client=client,
                prefix="continuous_data",
                filename=f"es_{timeframe}.csv",
                columns=TICK_BAR_COLUMNS,
                key_columns=("symbol", "timeframe", "timestamp"),
            )
            for timeframe in TICK_TIMEFRAMES
        }

    def run(self) -> None:
        self._install_signals()
        self._run_databento()

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
                    self._add_es_tick_bars(row)

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
                        for buffer in self._es_tick_buffers.values():
                            buffer.flush()
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
            for builder in self._es_tick_builders.values():
                for payload in builder.flush():
                    self._add_tick_bar_payload(payload)
            for buffer in self._es_tick_buffers.values():
                buffer.flush()
        except Exception:
            logger.exception("Final Databento flush failed")

    def _add_es_tick_bars(self, row: Mapping[str, Any]) -> None:
        timestamp = row.get("timestamp")
        if not isinstance(timestamp, datetime):
            return
        for timeframe, builder in self._es_tick_builders.items():
            payload = builder.update(
                symbol=str(row.get("symbol") or "ES"),
                price=float(row.get("price") or 0),
                size=float(row.get("size") or 0),
                timestamp=timestamp,
            )
            if payload is not None:
                self._add_tick_bar_payload(payload)

    def _add_tick_bar_payload(self, payload: Mapping[str, Any]) -> None:
        bar = payload.get("bar")
        if not isinstance(bar, Mapping):
            return
        timeframe = str(payload.get("timeframe") or "")
        buffer = self._es_tick_buffers.get(timeframe)
        if buffer is None:
            return
        buffer.add({
            "timestamp": bar.get("datetime", ""),
            "symbol": payload.get("symbol", ""),
            "timeframe": timeframe,
            "open": bar.get("open", ""),
            "high": bar.get("high", ""),
            "low": bar.get("low", ""),
            "close": bar.get("close", ""),
            "volume": bar.get("volume", ""),
            "end": bar.get("end", ""),
            "partial": bar.get("partial", False),
            "ticks": bar.get("ticks", int(timeframe[:-1])),
        })

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
