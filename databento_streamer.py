"""Databento live trade streamer.

Responsibility: Stream equity trades from Databento Live and aggregate them
into N-tick bars for StreamDataProcessor.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, time, timezone
from typing import TYPE_CHECKING, Any, Callable, Optional, Sequence

from config import normalize_market_symbol
from market_data_transformer import DATABENTO_STREAM_BAR_FIELDS
from market_session_scheduler import is_equity_streaming_session, parse_hhmm
from schwab_auth import _load_dotenv
from stream_data_processor import CleanBarEvent, StreamDataProcessor
from tick_bar_builder import TickBarBuilder, is_tick_timeframe, parse_tick_timeframe

if TYPE_CHECKING:
    from config import DatabentoSettings, StreamSettings

logger = logging.getLogger(__name__)


class DatabentoStreamSession:
    """Managed Databento Live session that emits completed N-tick OHLCV bars."""

    def __init__(
        self,
        *,
        api_key: str,
        symbols: Sequence[str],
        processor: StreamDataProcessor,
        dataset: str = "EQUS.MINI",
        schema: str = "trades",
        stype_in: str = "raw_symbol",
        ticks_per_bar: int = 50,
        market_timezone: str = "America/New_York",
        stream_start_local: time = time(4, 0),
        stream_end_local: time = time(20, 0),
        trading_days_only: bool = True,
        apply_equity_session_filter: bool = True,
        on_open_external: Optional[Callable[[], None]] = None,
        on_close_external: Optional[Callable[[], None]] = None,
        on_error_external: Optional[Callable[[Exception], None]] = None,
        on_trade: Optional[
            Callable[[str, datetime, float, float, str], None]
        ] = None,
        on_databento_trade: Optional[
            Callable[[Any, str, str], None]
        ] = None,
    ) -> None:
        if not api_key.strip():
            raise ValueError("Databento API key is required (set DATABENTO_API_KEY)")
        self._api_key = api_key.strip()
        self._symbols = tuple(normalize_market_symbol(symbol) for symbol in symbols)
        self._processor = processor
        self._dataset = dataset
        self._schema = schema
        self._stype_in = stype_in
        self._ticks_per_bar = ticks_per_bar
        self._market_timezone = market_timezone
        self._stream_start_local = stream_start_local
        self._stream_end_local = stream_end_local
        self._trading_days_only = trading_days_only
        self._apply_equity_session_filter = apply_equity_session_filter
        self._bar_builder = TickBarBuilder(ticks_per_bar=ticks_per_bar)
        self._on_open_external = on_open_external
        self._on_close_external = on_close_external
        self._on_error_external = on_error_external
        self._on_trade = on_trade
        self._on_databento_trade = on_databento_trade
        self._client: Any = None
        self._connected = False
        self._lock = threading.Lock()
        self._symbol_by_instrument_id: dict[int, str] = {}
        self._raw_symbol_by_instrument_id: dict[int, str] = {}
        self._outside_session_logged = False
        self._last_progress_log_at: dict[str, float] = {}
        self._trade_counts: dict[str, int] = {}

    @classmethod
    def from_env(
        cls,
        *,
        symbols: Sequence[str],
        processor: StreamDataProcessor,
        load_dotenv: bool = True,
        **kwargs: Any,
    ) -> DatabentoStreamSession:
        if load_dotenv:
            _load_dotenv()

        from config import get_config, secret

        app = get_config(reload=True)
        databento = app.databento
        historical = app.historical
        api_key = secret("DATABENTO_API_KEY") or databento.api_key
        ticks_per_bar = _ticks_per_bar_from_timeframe(
            app.market.stream_timeframe,
            fallback=databento.ticks_per_bar,
        )
        apply_filter = databento.apply_equity_session_filter
        if apply_filter is None:
            apply_filter = not databento.dataset.upper().startswith("GLBX")
        return cls(
            api_key=api_key,
            symbols=symbols,
            processor=processor,
            dataset=databento.dataset,
            schema=databento.schema,
            stype_in=databento.stype_in,
            ticks_per_bar=ticks_per_bar,
            market_timezone=app.app.timezone,
            stream_start_local=parse_hhmm(historical.extended_session_start_local),
            stream_end_local=parse_hhmm(historical.extended_session_end_local),
            trading_days_only=historical.trading_days_only,
            apply_equity_session_filter=bool(apply_filter),
            **kwargs,
        )

    @property
    def ticks_per_bar(self) -> int:
        return self._ticks_per_bar

    def connect(self) -> None:
        if self._connected:
            return

        try:
            import databento as db
        except ImportError as exc:
            raise ImportError(
                "databento package is required for stream_provider=databento. "
                "Install with: pip install databento"
            ) from exc

        client = db.Live(key=self._api_key)
        client.subscribe(
            dataset=self._dataset,
            schema=self._schema,
            symbols=list(self._symbols),
            stype_in=self._stype_in,
        )
        client.add_callback(self._on_record, exception_callback=self._on_callback_error)
        client.start()

        with self._lock:
            self._client = client
            self._connected = True

        logger.info(
            "Subscribed to Databento %s/%s for %s (%dt bars); "
            "accepting prints %s",
            self._dataset,
            self._schema,
            ", ".join(self._symbols),
            self._ticks_per_bar,
            (
                f"{self._stream_start_local.strftime('%H:%M')}-"
                f"{self._stream_end_local.strftime('%H:%M')} {self._market_timezone}"
                if self._apply_equity_session_filter
                else "all session hours (equity filter off)"
            ),
        )
        if self._on_open_external is not None:
            self._on_open_external()

    def flush_partial_bars(self) -> list[dict[str, Any]]:
        """Return and clear in-progress tick bars for shutdown persistence."""
        return self._bar_builder.flush()

    def _maybe_log_forming_progress(self, symbol: str, session_trades: int) -> None:
        """Log occasional forming-bar progress so thin sessions are visible."""
        import time as time_module

        forming = self._bar_builder.forming_ticks(symbol)
        if forming is None:
            return
        now = time_module.monotonic()
        with self._lock:
            last = self._last_progress_log_at.get(symbol, 0.0)
            if now - last < 30.0:
                return
            self._last_progress_log_at[symbol] = now
        logger.info(
            "Databento %s forming %d/%dt (trades_since_bar=%d)",
            symbol,
            forming,
            self._ticks_per_bar,
            session_trades,
        )

    def disconnect(self) -> None:
        client: Any = None
        with self._lock:
            client = self._client
            self._client = None
            self._connected = False
            self._symbol_by_instrument_id.clear()
            self._raw_symbol_by_instrument_id.clear()
            self._bar_builder.reset()

        if client is not None:
            try:
                client.stop()
            except Exception:
                logger.exception("Failed to stop Databento live client")
            try:
                client.terminate()
            except Exception:
                logger.debug("Databento terminate() unavailable or already closed")

        if self._on_close_external is not None:
            self._on_close_external()

    def _on_callback_error(self, exc: Exception) -> None:
        logger.exception("Databento callback failed: %s", exc)
        if self._on_error_external is not None:
            self._on_error_external(exc)

    def _on_record(self, record: Any) -> None:
        try:
            import databento as db
        except ImportError:
            return

        if isinstance(record, db.SymbolMappingMsg):
            self._handle_symbol_mapping(record)
            return

        if isinstance(record, db.ErrorMsg):
            message = getattr(record, "err", str(record))
            logger.error("Databento ErrorMsg: %s", message)
            if self._on_error_external is not None:
                self._on_error_external(RuntimeError(str(message)))
            return

        if isinstance(record, db.SystemMsg):
            logger.debug("Databento SystemMsg: %s", getattr(record, "msg", record))
            return

        if not isinstance(record, db.TradeMsg):
            return

        symbol = self._symbol_by_instrument_id.get(int(record.instrument_id))
        if symbol is None:
            return

        timestamp = _record_timestamp(record)
        if self._apply_equity_session_filter and not is_equity_streaming_session(
            timestamp,
            session_start_local=self._stream_start_local,
            session_end_local=self._stream_end_local,
            market_timezone=self._market_timezone,
            trading_days_only=self._trading_days_only,
        ):
            if not self._outside_session_logged:
                logger.info(
                    "Ignoring Databento trades outside equity streaming window "
                    "%s-%s %s (example ts=%s)",
                    self._stream_start_local.strftime("%H:%M"),
                    self._stream_end_local.strftime("%H:%M"),
                    self._market_timezone,
                    timestamp.isoformat(),
                )
                self._outside_session_logged = True
            return
        self._outside_session_logged = False

        price = float(getattr(record, "pretty_price", 0.0) or 0.0)
        if price <= 0:
            raw_price = getattr(record, "price", 0)
            if raw_price:
                price = float(raw_price) / 1_000_000_000.0
        size = float(getattr(record, "size", 0) or 0.0)
        side = str(getattr(record, "side", "") or "")

        if self._on_databento_trade is not None and price > 0:
            raw_symbol = self._raw_symbol_by_instrument_id.get(
                int(record.instrument_id), ""
            )
            try:
                self._on_databento_trade(record, symbol, raw_symbol)
            except Exception:
                logger.exception(
                    "Databento on_databento_trade callback failed for %s", symbol
                )

        if self._on_trade is not None and price > 0:
            try:
                self._on_trade(symbol, timestamp, price, size, side)
            except Exception:
                logger.exception("Databento on_trade callback failed for %s", symbol)

        payload = self._bar_builder.update(
            symbol=symbol,
            price=price,
            size=size,
            timestamp=timestamp,
        )
        with self._lock:
            self._trade_counts[symbol] = self._trade_counts.get(symbol, 0) + 1
            trade_count = self._trade_counts[symbol]
        if payload is None:
            self._maybe_log_forming_progress(symbol, trade_count)
            return
        with self._lock:
            self._trade_counts[symbol] = 0
            self._last_progress_log_at.pop(symbol, None)
        bar = payload.get("bar") or {}
        start_raw = bar.get("datetime")
        end_raw = bar.get("end") or timestamp.isoformat()
        duration_s = _bar_duration_seconds(start_raw, end_raw)
        logger.info(
            "Bar %s %dt @ %s duration=%.3fs O=%.2f H=%.2f L=%.2f C=%.2f V=%.0f",
            symbol,
            self._ticks_per_bar,
            start_raw,
            duration_s,
            float(bar.get("open") or 0.0),
            float(bar.get("high") or 0.0),
            float(bar.get("low") or 0.0),
            float(bar.get("close") or price),
            float(bar.get("volume") or 0.0),
        )
        try:
            self._processor.process_message(json.dumps(payload))
        except Exception:
            logger.exception(
                "Databento bar processor failed for %s; stream continues",
                symbol,
            )

    def _handle_symbol_mapping(self, record: Any) -> None:
        instrument_id = int(record.instrument_id)
        in_sym = str(getattr(record, "stype_in_symbol", None) or "").strip()
        out_sym = str(getattr(record, "stype_out_symbol", None) or "").strip()
        raw_symbol = in_sym or out_sym
        if not raw_symbol:
            return
        symbol = normalize_market_symbol(raw_symbol)
        with self._lock:
            self._symbol_by_instrument_id[instrument_id] = symbol
            self._raw_symbol_by_instrument_id[instrument_id] = out_sym or raw_symbol
        logger.info(
            "Databento mapped instrument_id=%s -> %s%s",
            instrument_id,
            symbol,
            f" (raw={out_sym})" if out_sym and out_sym != symbol else "",
        )


def build_databento_stream_processor(
    *,
    symbols: Sequence[str],
    consumers: Optional[list[Callable[[CleanBarEvent], None]]] = None,
    timeframe: str = "50t",
    require_minute_alignment: Optional[bool] = None,
    dedup_window: Optional[int] = None,
    stream_settings: Optional["StreamSettings"] = None,
) -> StreamDataProcessor:
    from config import StreamSettings, get_config

    stream = stream_settings or get_config().stream
    if require_minute_alignment is None:
        # Tick bars are not minute-aligned; default off unless explicitly set.
        if is_tick_timeframe(timeframe):
            require_minute_alignment = False
        else:
            require_minute_alignment = stream.require_minute_alignment
    if dedup_window is None:
        dedup_window = stream.dedup_window
    return StreamDataProcessor(
        symbols=symbols,
        timeframe=timeframe,
        consumers=consumers or [],
        field_map=DATABENTO_STREAM_BAR_FIELDS,
        bar_key="bar",
        symbol_key="symbol",
        timeframe_key="timeframe",
        require_minute_alignment=require_minute_alignment,
        dedup_window=dedup_window,
    )


def _ticks_per_bar_from_timeframe(timeframe: str, *, fallback: int) -> int:
    if is_tick_timeframe(timeframe):
        return parse_tick_timeframe(timeframe)
    return fallback


def _bar_duration_seconds(start_raw: Any, end_raw: Any) -> float:
    """Return last-tick minus first-tick duration in seconds."""
    try:
        start = datetime.fromisoformat(str(start_raw).replace("Z", "+00:00"))
        end = datetime.fromisoformat(str(end_raw).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return 0.0
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    return max((end - start).total_seconds(), 0.0)


def _record_timestamp(record: Any) -> datetime:
    ts_event = getattr(record, "ts_event", None)
    if ts_event is None:
        return datetime.now(timezone.utc)
    # Databento timestamps are nanoseconds since epoch.
    if isinstance(ts_event, int):
        return datetime.fromtimestamp(ts_event / 1_000_000_000, tz=timezone.utc)
    pretty = getattr(record, "pretty_ts_event", None)
    if pretty is not None:
        if isinstance(pretty, datetime):
            if pretty.tzinfo is None:
                return pretty.replace(tzinfo=timezone.utc)
            return pretty.astimezone(timezone.utc)
        return datetime.fromisoformat(str(pretty).replace("Z", "+00:00"))
    return datetime.now(timezone.utc)
