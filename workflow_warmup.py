"""Workflow startup warmup from GCS and historical backfill.

Responsibility: Sync missing strategy-timeframe OHLCV into storage and replay
recent bars to warm indicator state before the live 1m stream starts.
"""

from __future__ import annotations

import logging
import os
import calendar
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import TYPE_CHECKING, Optional

import pandas as pd

from bar_alignment import (
    aggregation_checkpoint,
    align_bucket_start,
    is_aligned_timestamp,
    last_completed_bar_timestamp,
    last_completed_minute,
    timeframe_minutes,
    to_utc,
)
from backfill_executor import BackfillRequest
from cloud_storage_repository import CloudStorageRepository
from local_storage_repository import (
    LayeredOhlcvRepository,
    LocalParquetRepository,
    gcs_bucket_exists,
)
from data_aggregator import AggregatedBar
from historical_orchestrator import HistoricalOrchestrator
from ohlc_sanity import repair_ohlcv_dataframe
from ohlcv_schema import OHLCV_COLUMNS
from schwab_market_data_client import build_schwab_backfill_executor
from stream_data_processor import CleanBarEvent

if TYPE_CHECKING:
    from config import AppConfig
    from workflow import TradingWorkflow

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WarmupSummary:
    """Outcome of a startup storage sync and indicator warmup."""

    symbol: str
    sync_requests: int
    bars_written: int
    bars_replayed: int
    warmup_start: datetime
    warmup_end: datetime
    storage_timeframe: str


@dataclass(frozen=True)
class GexWarmupSummary:
    """Outcome of GEX-specific startup preload."""

    symbol: str
    volumes_seeded: int
    lookback_bars: int
    source: str


def indicator_warmup_needed(app: "AppConfig", strategies: tuple[str, ...]) -> bool:
    """Return True when startup should run the full historical indicator warmup."""
    non_gex = set(strategies) - {"gex_scalp"}
    if non_gex:
        return True
    jobs = app.indicators.build_jobs(app.market.strategy_timeframe)
    return bool(jobs)


def fetch_recent_1m_volumes(
    app: "AppConfig",
    symbol: str,
    *,
    lookback_bars: int,
    end: datetime,
    storage: Optional[CloudStorageRepository] = None,
    executor: Optional[object] = None,
) -> tuple[list[float], str]:
    """Load recent 1m volumes from storage, falling back to a REST backfill."""
    symbol = symbol.upper()
    stream_timeframe = app.market.stream_timeframe
    lookback_bars = max(lookback_bars, 1)
    end = _to_utc(end)
    floor = end - warmup_lookback_duration_for_bars(
        stream_timeframe,
        lookback_bars + 10,
    )

    bars = _empty_ohlcv_frame()
    source = "none"
    if storage is not None:
        bars = load_recent_stored_bars(
            storage,
            symbol,
            stream_timeframe,
            end=end,
            required_bars=lookback_bars,
            floor=floor,
            use_daily_partitions=app.gcs.use_daily_partitions,
        )
        if not bars.empty:
            source = "storage"

    if len(bars) < lookback_bars and executor is not None and storage is not None:
        request_start = max(floor, end - warmup_lookback_duration_for_bars(
            stream_timeframe,
            lookback_bars + 30,
        ))
        request = BackfillRequest(
            symbol=symbol,
            timeframe=stream_timeframe,
            start=request_start,
            end=end + timedelta(minutes=1),
            partition_date=end.date() if app.gcs.use_daily_partitions else None,
        )
        try:
            logger.info(
                "Fetching recent 1m bars for GEX volume seed (%s): %s -> %s",
                symbol,
                request_start.isoformat(),
                end.isoformat(),
            )
            executor.execute(request)
            bars = load_recent_stored_bars(
                storage,
                symbol,
                stream_timeframe,
                end=end,
                required_bars=lookback_bars,
                floor=floor,
                use_daily_partitions=app.gcs.use_daily_partitions,
            )
            if not bars.empty:
                source = "rest"
        except Exception:
            logger.exception("REST fetch for GEX volume seed failed for %s", symbol)

    if bars.empty:
        return [], source

    volumes = (
        bars.sort_values("timestamp")["volume"].astype(float).tolist()[-lookback_bars:]
    )
    return volumes, source


def warm_start_gex(workflow: "TradingWorkflow") -> list[GexWarmupSummary]:
    """Seed gex_scalp volume lookback from recent 1m bars before the live stream."""
    app = workflow.config.app
    gex = app.gex
    if not gex.enabled or "gex_scalp" not in workflow.config.strategies:
        return []
    if not gex.seed_volume_history:
        return []

    lookback_bars = max(gex.volume_lookback_bars, 1)
    end = last_completed_minute()
    logger.info(
        "=== GEX startup: seeding %d-bar 1m volume lookback (through %s) ===",
        lookback_bars,
        end.isoformat(),
    )

    storage: Optional[CloudStorageRepository] = None
    executor: Optional[object] = None
    try:
        storage = build_storage_repository(app)
        executor = build_backfill_executor(storage, app=app)
    except Exception:
        logger.exception(
            "GEX volume seed storage/backfill unavailable; continuing without preload"
        )

    summaries: list[GexWarmupSummary] = []
    try:
        for symbol in workflow.symbols:
            volumes, source = fetch_recent_1m_volumes(
                app,
                symbol,
                lookback_bars=lookback_bars,
                end=end,
                storage=storage,
                executor=executor,
            )
            seeded = workflow.seed_gex_volume_history(symbol, volumes)
            if seeded:
                logger.info(
                    "GEX volume seed for %s: %d/%d bar(s) from %s",
                    symbol,
                    seeded,
                    lookback_bars,
                    source,
                )
            else:
                logger.warning(
                    "GEX volume seed for %s: no 1m history available; "
                    "volume-spike filter activates after %d live bar(s)",
                    symbol,
                    lookback_bars,
                )
            summaries.append(
                GexWarmupSummary(
                    symbol=symbol,
                    volumes_seeded=seeded,
                    lookback_bars=lookback_bars,
                    source=source if seeded else "none",
                )
            )
    finally:
        if executor is not None:
            close_backfill_executor(executor)

    return summaries


def warm_start_pipeline(workflow: "TradingWorkflow") -> list[WarmupSummary]:
    """Backfill gaps through the last completed bar and warm indicator buffers."""
    app = workflow.config.app
    if not app.workflow.warmup_from_storage:
        return []

    logger.info("=== Startup preload: checking GCS and backfilling gaps ===")

    summaries: list[WarmupSummary] = []
    orchestrator = None
    try:
        gcs = app.gcs
        logger.info(
            "Connecting to GCS bucket gs://%s/%s",
            gcs.bucket_name,
            gcs.ohlcv_prefix,
        )
        storage = build_storage_repository(app)
        orchestrator = build_historical_orchestrator(app, storage)
    except Exception:
        logger.exception("Storage warmup unavailable; starting live stream without preload")
        return []

    storage_timeframe = backfill_timeframe(app)
    backfill_end = last_completed_bar_timestamp(storage_timeframe)
    last_minute = last_completed_minute()
    sync_start, warmup_start, bootstrap_start = startup_sync_window(
        app,
        end=backfill_end,
        timeframe=storage_timeframe,
    )
    session_start, session_end = effective_session_times(app)
    session_label = (
        f"extended {session_start.strftime('%H:%M')}-{session_end.strftime('%H:%M')} UTC"
        if app.historical.need_extended_hours
        else f"regular {session_start.strftime('%H:%M')}-{session_end.strftime('%H:%M')} UTC"
    )
    logger.info(
        "Preload plan: timeframe=%s | session=%s | backfill scan %s -> %s | bootstrap if empty from %s | indicator replay from %s",
        storage_timeframe,
        session_label,
        sync_start.isoformat(),
        backfill_end.isoformat(),
        bootstrap_start.isoformat(),
        warmup_start.isoformat(),
    )

    for symbol in workflow.symbols:
        try:
            logger.info("--- %s preload begin ---", symbol)
            stored_range_start = bootstrap_start.date()
            stored_latest_before = latest_stored_bar_timestamp(
                storage,
                symbol,
                storage_timeframe,
                use_daily_partitions=app.gcs.use_daily_partitions,
                range_start=stored_range_start,
            )
            if stored_latest_before is not None:
                logger.info(
                    "Last stored %s bar for %s before backfill: %s",
                    storage_timeframe,
                    symbol,
                    stored_latest_before.isoformat(),
                )
            else:
                logger.info(
                    "No stored %s data for %s since %s; will bootstrap missing history",
                    storage_timeframe,
                    symbol,
                    stored_range_start.isoformat(),
                )

            logger.info(
                "Planning historical backfill for missing %s gaps (%s -> %s)",
                storage_timeframe,
                sync_start.isoformat(),
                backfill_end.isoformat(),
            )
            plan, results = orchestrator.run(
                symbol,
                storage_timeframe,
                sync_start,
                backfill_end,
                bootstrap_start=bootstrap_start,
            )
            bars_written = sum(result.rows_written for result in results)
            logger.info(
                "Backfill complete for %s: %d request(s), %d row(s) fetched/written to GCS",
                symbol,
                len(plan.backfill_requests),
                bars_written,
            )
            if not plan.backfill_requests:
                logger.info(
                    "No %s gaps detected for %s in %s -> %s",
                    storage_timeframe,
                    symbol,
                    sync_start.isoformat(),
                    backfill_end.isoformat(),
                )

            stored_latest = latest_stored_bar_timestamp(
                storage,
                symbol,
                storage_timeframe,
                use_daily_partitions=app.gcs.use_daily_partitions,
                range_start=stored_range_start,
            )
            replay_end = backfill_end
            if stored_latest is not None and stored_latest > replay_end:
                replay_end = stored_latest

            required_bars = warmup_required_bar_count(app)
            logger.info(
                "Loading up to %d stored %s bar(s) for indicator warmup (ending %s)",
                required_bars,
                storage_timeframe,
                replay_end.isoformat(),
            )
            bars = load_recent_stored_bars(
                storage,
                symbol,
                storage_timeframe,
                end=replay_end,
                required_bars=required_bars,
                floor=bootstrap_start,
                use_daily_partitions=app.gcs.use_daily_partitions,
            )
            if bars.empty:
                logger.warning(
                    "No stored %s bars available for %s indicator warmup",
                    storage_timeframe,
                    symbol,
                )
            else:
                first_ts = to_utc(pd.Timestamp(bars["timestamp"].min()).to_pydatetime())
                logger.info(
                    "Loaded %d stored %s bar(s) for %s replay (%s -> %s)",
                    len(bars),
                    storage_timeframe,
                    symbol,
                    first_ts.isoformat(),
                    replay_end.isoformat(),
                )
            replayed = replay_warmup_bars_for_symbol(
                workflow,
                symbol=symbol,
                timeframe=storage_timeframe,
                bars=bars,
            )
            logger.info(
                "Indicator warmup complete for %s: replayed %d %s bar(s)",
                symbol,
                replayed,
                storage_timeframe,
            )
            if replayed < required_bars:
                logger.warning(
                    "Supertrend warmup incomplete for %s: replayed %d/%d bar(s); "
                    "signals may show 'warming up' until more history is stored",
                    symbol,
                    replayed,
                    required_bars,
                )
            if not bars.empty:
                last_saved = to_utc(pd.Timestamp(bars["timestamp"].max()).to_pydatetime())
                minute_rows = sync_stream_minute_tail(
                    workflow,
                    storage,
                    app,
                    symbol,
                    last_saved_3m=last_saved,
                    end=last_minute,
                )
                if minute_rows:
                    logger.info(
                        "Fetched %d stored 1m bar(s) after last %s candle for live aggregation",
                        minute_rows,
                        storage_timeframe,
                    )
                workflow.seed_live_aggregation_from_storage(
                    symbol,
                    last_saved,
                    storage,
                    end=last_minute,
                )
            logger.info("--- %s preload complete ---", symbol)
            summaries.append(
                WarmupSummary(
                    symbol=symbol,
                    sync_requests=len(plan.backfill_requests),
                    bars_written=bars_written,
                    bars_replayed=replayed,
                    warmup_start=warmup_start,
                    warmup_end=replay_end,
                    storage_timeframe=storage_timeframe,
                )
            )
        except Exception:
            logger.exception("Warmup failed for %s; continuing with live stream", symbol)

    if summaries:
        total_written = sum(summary.bars_written for summary in summaries)
        total_replayed = sum(summary.bars_replayed for summary in summaries)
        logger.info(
            "=== Startup preload complete: %d bar(s) written to GCS, %d bar(s) replayed ===",
            total_written,
            total_replayed,
        )
    else:
        logger.info("=== Startup preload finished with no symbol summaries ===")

    if orchestrator is not None:
        close_backfill_executor(orchestrator._backfill_executor)

    return summaries


def sync_stream_minute_tail(
    workflow: "TradingWorkflow",
    storage: CloudStorageRepository,
    app: "AppConfig",
    symbol: str,
    *,
    last_saved_3m: datetime,
    end: datetime,
) -> int:
    """Backfill 1m bars after the last stored 3m candle through the last completed minute.

    The open 3m bucket is intentionally not written here; ``seed_live_aggregation``
    replays these 1m bars and the live stream finishes the bucket on the closing
    minute.
    """
    del workflow

    strategy_timeframe = app.market.strategy_timeframe
    stream_timeframe = app.market.stream_timeframe
    _, seed_start = aggregation_checkpoint(
        last_saved_3m,
        timeframe=strategy_timeframe,
        now=end,
    )
    end_minute = last_completed_minute(end)
    if seed_start > end_minute:
        return 0

    executor = build_backfill_executor(storage, app=app)
    rows_written = 0
    cursor_day = seed_start.date()
    while cursor_day <= end_minute.date():
        day_start = datetime.combine(cursor_day, time.min, tzinfo=timezone.utc)
        day_end = datetime.combine(
            cursor_day + timedelta(days=1),
            time.min,
            tzinfo=timezone.utc,
        )
        request_start = max(seed_start, day_start)
        request_end = min(end_minute + timedelta(minutes=1), day_end)
        if request_start >= request_end:
            cursor_day += timedelta(days=1)
            continue

        request = BackfillRequest(
            symbol=symbol,
            timeframe=stream_timeframe,
            start=request_start,
            end=request_end,
            partition_date=cursor_day if app.gcs.use_daily_partitions else None,
        )
        if executor._request_satisfied_by_storage(request):
            try:
                stored = storage.read(
                    symbol,
                    stream_timeframe,
                    partition_date=cursor_day if app.gcs.use_daily_partitions else None,
                )
                start_ts = pd.Timestamp(request_start)
                end_ts = pd.Timestamp(request_end)
                covered = stored[
                    (stored["timestamp"] >= start_ts)
                    & (stored["timestamp"] < end_ts)
                ]
                rows_written += len(covered)
            except FileNotFoundError:
                pass
            cursor_day += timedelta(days=1)
            continue

        logger.info(
            "Fetching 1m tail for %s aggregation: %s -> %s",
            symbol,
            request_start.isoformat(),
            request_end.isoformat(),
        )
        result = executor.execute(request)
        rows_written += result.rows_written
        cursor_day += timedelta(days=1)

    close_backfill_executor(executor)
    return rows_written


def backfill_timeframe(app: "AppConfig") -> str:
    """Return the timeframe used for historical storage and backfill."""
    return app.historical.timeframe


def replay_warmup_bars_for_symbol(
    workflow: "TradingWorkflow",
    *,
    symbol: str,
    timeframe: str,
    bars: pd.DataFrame,
) -> int:
    """Replay stored strategy-timeframe bars into indicator buffers."""
    if bars.empty:
        return 0

    replayed = 0
    symbol = symbol.upper()
    strategy_timeframe = workflow.config.market_config.strategy_timeframe

    for row in bars.itertuples(index=False):
        timestamp = to_utc(pd.Timestamp(row.timestamp).to_pydatetime())
        if not is_aligned_timestamp(timestamp, timeframe):
            aligned = align_bucket_start(timestamp, timeframe)
            logger.warning(
                "Stored %s %s bar timestamp %s is not bucket-aligned; using %s",
                symbol,
                timeframe,
                timestamp.isoformat(),
                aligned.isoformat(),
            )
            timestamp = aligned

        if timeframe == strategy_timeframe:
            workflow.replay_warmup_aggregated_bar(
                AggregatedBar(
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=timestamp,
                    open=float(row.open),
                    high=float(row.high),
                    low=float(row.low),
                    close=float(row.close),
                    volume=float(row.volume),
                    is_complete=True,
                )
            )
        else:
            workflow.replay_warmup_bar(
                CleanBarEvent(
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=timestamp,
                    open=float(row.open),
                    high=float(row.high),
                    low=float(row.low),
                    close=float(row.close),
                    volume=float(row.volume),
                )
            )
        replayed += 1
    return replayed


def load_stored_bars(
    storage: CloudStorageRepository,
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    *,
    use_daily_partitions: bool,
) -> pd.DataFrame:
    """Load OHLCV rows from GCS across daily partitions."""
    symbol = symbol.upper()
    start = _to_utc(start)
    end = _to_utc(end)

    if not use_daily_partitions:
        try:
            return storage.read(symbol, timeframe, start=start, end=end)
        except FileNotFoundError:
            return _empty_ohlcv_frame()

    frames: list[pd.DataFrame] = []
    day = start.date()
    while day <= end.date():
        if storage.exists(symbol, timeframe, partition_date=day):
            try:
                frame = storage.read(
                    symbol,
                    timeframe,
                    start=start,
                    end=end,
                    partition_date=day,
                )
            except FileNotFoundError:
                frame = _empty_ohlcv_frame()
            if not frame.empty:
                frames.append(frame)
        day += timedelta(days=1)

    if not frames:
        return _empty_ohlcv_frame()

    merged = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["timestamp"], keep="last")
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    if start_ts.tzinfo is None:
        start_ts = start_ts.tz_localize("UTC")
    else:
        start_ts = start_ts.tz_convert("UTC")
    if end_ts.tzinfo is None:
        end_ts = end_ts.tz_localize("UTC")
    else:
        end_ts = end_ts.tz_convert("UTC")
    return repair_ohlcv_dataframe(
        merged[
            (merged["timestamp"] >= start_ts) & (merged["timestamp"] <= end_ts)
        ].reset_index(drop=True)
    )


def load_recent_stored_bars(
    storage: CloudStorageRepository,
    symbol: str,
    timeframe: str,
    *,
    end: datetime,
    required_bars: int,
    floor: datetime,
    use_daily_partitions: bool,
) -> pd.DataFrame:
    """Load the most recent ``required_bars`` stored bars, walking back by day."""
    symbol = symbol.upper()
    end = _to_utc(end)
    floor = _to_utc(floor)

    if required_bars <= 0:
        return _empty_ohlcv_frame()

    if not use_daily_partitions:
        start = max(floor, end - warmup_lookback_duration_for_bars(timeframe, required_bars))
        return load_stored_bars(
            storage,
            symbol,
            timeframe,
            start,
            end,
            use_daily_partitions=False,
        )

    frames: list[pd.DataFrame] = []
    total_rows = 0
    day = end.date()
    floor_day = floor.date()

    while day >= floor_day and total_rows < required_bars:
        if storage.exists(symbol, timeframe, partition_date=day):
            try:
                partition_start = floor if day == floor_day else None
                frame = storage.read(
                    symbol,
                    timeframe,
                    start=partition_start,
                    end=end,
                    partition_date=day,
                )
            except FileNotFoundError:
                frame = _empty_ohlcv_frame()
            if not frame.empty:
                frames.insert(0, frame)
                total_rows += len(frame)
        day -= timedelta(days=1)

    if not frames:
        return _empty_ohlcv_frame()

    merged = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["timestamp"], keep="last")
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    end_ts = pd.Timestamp(end)
    if end_ts.tzinfo is None:
        end_ts = end_ts.tz_localize("UTC")
    else:
        end_ts = end_ts.tz_convert("UTC")
    floor_ts = pd.Timestamp(floor)
    if floor_ts.tzinfo is None:
        floor_ts = floor_ts.tz_localize("UTC")
    else:
        floor_ts = floor_ts.tz_convert("UTC")
    merged = merged[
        (merged["timestamp"] >= floor_ts) & (merged["timestamp"] <= end_ts)
    ].reset_index(drop=True)
    if len(merged) > required_bars:
        merged = merged.tail(required_bars).reset_index(drop=True)
    return merged


def warmup_lookback_duration_for_bars(timeframe: str, required_bars: int) -> timedelta:
    """Return a time span that should cover ``required_bars`` for ``timeframe``."""
    bar_minutes = _timeframe_minutes(timeframe)
    return timedelta(minutes=required_bars * bar_minutes + bar_minutes)


def effective_session_times(app: "AppConfig") -> tuple[time, time]:
    """Return UTC session bounds used for backfill and gap detection."""
    historical = app.historical
    if historical.need_extended_hours:
        return (
            _parse_utc_time(historical.extended_session_start_utc),
            _parse_utc_time(historical.extended_session_end_utc),
        )
    return (
        _parse_utc_time(historical.session_start_utc),
        _parse_utc_time(historical.session_end_utc),
    )


def configured_sync_start(app: "AppConfig") -> datetime:
    """Return the earliest configured historical sync timestamp."""
    historical = app.historical
    session_start, _ = effective_session_times(app)
    return datetime.combine(
        date.fromisoformat(historical.sync_start_date),
        session_start,
        tzinfo=timezone.utc,
    )


def bootstrap_sync_start(app: "AppConfig", end: datetime) -> datetime:
    """Return the rolling earliest timestamp for empty-storage bootstrap."""
    historical = app.historical
    end = _to_utc(end)
    months = historical.bootstrap_lookback_months
    end_day = end.date()
    month = end_day.month - months
    year = end_day.year
    while month <= 0:
        month += 12
        year -= 1
    last_day = calendar.monthrange(year, month)[1]
    start_day = date(year, month, min(end_day.day, last_day))
    session_start, _ = effective_session_times(app)
    rolling_start = datetime.combine(
        start_day,
        session_start,
        tzinfo=timezone.utc,
    )
    configured_floor = configured_sync_start(app)
    return max(rolling_start, configured_floor)


def startup_sync_window(
    app: "AppConfig",
    *,
    end: datetime,
    timeframe: str,
) -> tuple[datetime, datetime, datetime]:
    """Return (backfill_scan_start, warmup_start, bootstrap_start)."""
    warmup_duration = warmup_lookback_duration(app, timeframe)

    configured_start = configured_sync_start(app)
    bootstrap_start = max(bootstrap_sync_start(app, end), configured_start)
    warmup_start = max(bootstrap_start, end - warmup_duration)
    return bootstrap_start, warmup_start, bootstrap_start


def warmup_lookback_duration(app: "AppConfig", timeframe: str) -> timedelta:
    """Estimate how far back to load stored bars for indicator warmup."""
    return warmup_lookback_duration_for_bars(
        timeframe,
        warmup_required_bar_count(app),
    )


def warmup_required_bar_count(app: "AppConfig") -> int:
    """Return the number of strategy-timeframe bars needed for warmup."""
    required_bars = app.indicators.max_bars

    if app.indicators.dema is not None:
        required_bars = max(required_bars, app.indicators.dema.period)

    if app.indicators.supertrend is not None:
        required_bars = max(
            required_bars,
            app.indicators.supertrend.atr_period + 5,
        )

    return required_bars


def latest_stored_bar_timestamp(
    storage: CloudStorageRepository,
    symbol: str,
    timeframe: str,
    *,
    use_daily_partitions: bool,
    range_start: Optional[date] = None,
    lookback_days: int = 7,
) -> Optional[datetime]:
    """Return the newest stored bar timestamp for a symbol/timeframe."""
    symbol = symbol.upper()
    if not use_daily_partitions:
        try:
            frame = storage.read(symbol, timeframe)
        except FileNotFoundError:
            return None
        if frame.empty:
            return None
        return to_utc(pd.Timestamp(frame["timestamp"].max()).to_pydatetime())

    latest: Optional[datetime] = None
    today = datetime.now(timezone.utc).date()
    first_day = range_start or (today - timedelta(days=lookback_days))
    day = today
    while day >= first_day:
        if storage.exists(symbol, timeframe, partition_date=day):
            try:
                frame = storage.read(symbol, timeframe, partition_date=day)
            except FileNotFoundError:
                frame = pd.DataFrame()
            if not frame.empty:
                candidate = to_utc(pd.Timestamp(frame["timestamp"].max()).to_pydatetime())
                if latest is None or candidate > latest:
                    latest = candidate
        day -= timedelta(days=1)
    return latest


def build_storage_repository(app: "AppConfig") -> LayeredOhlcvRepository:
    """Build layered local + GCS OHLCV storage from application config."""
    gcs = app.gcs
    if gcs.credentials_path:
        os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", gcs.credentials_path)
    if gcs.project_id:
        os.environ.setdefault("GOOGLE_CLOUD_PROJECT", gcs.project_id)

    remote = CloudStorageRepository(
        gcs.bucket_name,
        prefix=gcs.ohlcv_prefix,
    )
    local = LocalParquetRepository(
        gcs.local_fallback_path,
        prefix=gcs.ohlcv_prefix,
    )
    remote_enabled = gcs_bucket_exists(gcs.bucket_name, remote._client)
    if remote_enabled:
        logger.info(
            "OHLCV storage: local %s/%s with GCS replication to gs://%s/%s",
            gcs.local_fallback_path,
            gcs.ohlcv_prefix,
            gcs.bucket_name,
            gcs.ohlcv_prefix,
        )
    else:
        logger.warning(
            "GCS bucket gs://%s not found; OHLCV will be stored locally under %s/%s",
            gcs.bucket_name,
            gcs.local_fallback_path,
            gcs.ohlcv_prefix,
        )

    return LayeredOhlcvRepository(
        local,
        remote,
        remote_enabled=remote_enabled,
    )


def build_backfill_executor(
    storage: CloudStorageRepository,
    app: "AppConfig",
):
    """Return the market-data backfill executor for the configured provider."""
    if app.workflow.stream_provider == "ibkr" or app.broker.provider == "ibkr":
        from ibkr_tws_market_data_client import build_ibkr_tws_backfill_executor

        return build_ibkr_tws_backfill_executor(storage, app=app)
    return build_schwab_backfill_executor(storage, app=app)


def close_backfill_executor(executor: object) -> None:
    """Release any provider-specific backfill client connections."""
    client = getattr(executor, "api_client", None)
    if client is not None and hasattr(client, "close"):
        client.close()


def build_historical_orchestrator(
    app: "AppConfig",
    storage: CloudStorageRepository,
) -> HistoricalOrchestrator:
    """Build the historical sync orchestrator for startup backfill."""
    historical = app.historical
    return HistoricalOrchestrator(
        storage,
        build_backfill_executor(storage, app=app),
        use_daily_partitions=app.gcs.use_daily_partitions,
        session_start=_parse_utc_time(historical.session_start_utc),
        session_end=_parse_utc_time(historical.session_end_utc),
        need_extended_hours=historical.need_extended_hours,
        extended_session_start=_parse_utc_time(
            historical.extended_session_start_utc
        ),
        extended_session_end=_parse_utc_time(historical.extended_session_end_utc),
        trading_days_only=historical.trading_days_only,
        bootstrap_if_empty=historical.bootstrap_if_empty,
    )


def _parse_utc_time(value: str) -> time:
    hour, minute = value.split(":", 1)
    return time(int(hour), int(minute))


def _to_utc(value: datetime) -> datetime:
    return to_utc(value)


def _timeframe_minutes(timeframe: str) -> int:
    return timeframe_minutes(timeframe)


def _empty_ohlcv_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(OHLCV_COLUMNS))
