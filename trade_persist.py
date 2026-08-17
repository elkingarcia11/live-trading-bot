"""Non-blocking raw-trade durability: queue + background writer process.

The ingest/trade callback stays in the parent process. It only ``put_nowait``s
pickleable row dicts. The writer process batches those rows into local parquet
parts and periodically compact/merges them into daily partitions + GCS.
``queue.Full`` drops the row; ingest never blocks on disk or network I/O.
"""

from __future__ import annotations

import logging
import multiprocessing
import os
import signal
import time
from dataclasses import dataclass
from queue import Empty, Full
from typing import TYPE_CHECKING, Any, Mapping, Optional

from google.cloud import storage

from local_storage_repository import gcs_bucket_exists
from session_trade_recorder import SessionTradeRecorder
from trade_capture_runtime import FlushPolicy

if TYPE_CHECKING:
    from config import AppConfig

logger = logging.getLogger(__name__)

_GET_TIMEOUT_S = 0.25
_STATS_QUEUE_MAX = 1000
# macOS SEM_VALUE_MAX is typically 32767; keep well under that.
DEFAULT_QUEUE_MAXSIZE = 16_384
DEFAULT_COMPACT_INTERVAL_S = 300.0
DEFAULT_SHUTDOWN_TIMEOUT_S = 120.0


@dataclass(frozen=True)
class TradePersistWriterConfig:
    """Pickleable settings for the background persist process."""

    local_root: str
    bucket_name: str
    prefix: str = "trades"
    use_daily_partitions: bool = True
    remote_enabled: bool = True
    credentials_path: str = ""
    project_id: str = ""
    every_rows: int = 5000
    interval_seconds: float = 0.0
    compact_interval_seconds: float = DEFAULT_COMPACT_INTERVAL_S
    log_level: int = logging.INFO

    @classmethod
    def from_app(
        cls,
        app: AppConfig,
        *,
        every_rows: int = 5000,
        interval_seconds: float = 0.0,
        compact_interval_seconds: float = DEFAULT_COMPACT_INTERVAL_S,
    ) -> TradePersistWriterConfig:
        gcs = app.gcs
        if gcs.credentials_path:
            os.environ.setdefault(
                "GOOGLE_APPLICATION_CREDENTIALS", gcs.credentials_path
            )
        if gcs.project_id:
            os.environ.setdefault("GOOGLE_CLOUD_PROJECT", gcs.project_id)

        client = storage.Client()
        remote_enabled = gcs_bucket_exists(gcs.bucket_name, client)
        prefix = gcs.trades_prefix or "trades"
        if remote_enabled:
            logger.info(
                "Raw trade persist: local %s/%s with GCS replication to gs://%s/%s",
                gcs.local_fallback_path,
                prefix,
                gcs.bucket_name,
                prefix,
            )
        else:
            logger.warning(
                "GCS bucket gs://%s not found; persist writer will store locally "
                "under %s/%s",
                gcs.bucket_name,
                gcs.local_fallback_path,
                prefix,
            )
        return cls(
            local_root=str(gcs.local_fallback_path),
            bucket_name=gcs.bucket_name,
            prefix=prefix,
            use_daily_partitions=gcs.use_daily_partitions,
            remote_enabled=remote_enabled,
            credentials_path=gcs.credentials_path or "",
            project_id=gcs.project_id or "",
            every_rows=max(0, int(every_rows)),
            interval_seconds=max(0.0, float(interval_seconds)),
            compact_interval_seconds=max(0.0, float(compact_interval_seconds)),
        )


class TradePersistClient:
    """Parent-side handle: non-blocking put + lifecycle for the writer process."""

    def __init__(
        self,
        queue: Any,
        stop_event: Any,
        stats_queue: Any,
        process: Optional[Any] = None,
    ) -> None:
        self._queue = queue
        self._stop = stop_event
        self._stats = stats_queue
        self._process = process
        self._closed = False

    @classmethod
    def start(
        cls,
        config: TradePersistWriterConfig,
        *,
        queue_maxsize: int = DEFAULT_QUEUE_MAXSIZE,
    ) -> TradePersistClient:
        ctx = multiprocessing.get_context("spawn")
        queue = ctx.Queue(maxsize=max(1, int(queue_maxsize)))
        stop_event = ctx.Event()
        stats_queue = ctx.Queue(maxsize=_STATS_QUEUE_MAX)
        process = ctx.Process(
            target=run_trade_persist_writer,
            args=(queue, stop_event, stats_queue, config),
            name="trade-persist-writer",
            daemon=False,
        )
        process.start()
        logger.info(
            "Trade persist writer pid=%s queue_maxsize=%d flush_every=%s "
            "flush_interval=%ss compact_interval=%ss",
            process.pid,
            queue_maxsize,
            config.every_rows or "∞",
            config.interval_seconds or "off",
            config.compact_interval_seconds or "shutdown-only",
        )
        return cls(queue, stop_event, stats_queue, process)

    def try_put(self, row: Mapping[str, Any]) -> bool:
        """Enqueue one trade row without blocking. False means dropped (queue full)."""
        if self._closed:
            return False
        try:
            self._queue.put_nowait(row)
            return True
        except Full:
            return False

    def approx_queued(self) -> int:
        try:
            return int(self._queue.qsize())
        except (NotImplementedError, OverflowError, AttributeError, OSError):
            return 0

    def poll_stats(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        while True:
            try:
                events.append(self._stats.get_nowait())
            except Empty:
                break
            except Exception:
                break
        return events

    def shutdown(self, *, timeout: float = DEFAULT_SHUTDOWN_TIMEOUT_S) -> None:
        """Stop the writer after it drains, appends, and compact/merges."""
        if self._closed:
            return
        self._stop.set()
        if self._process is not None:
            self._process.join(timeout)
            if self._process.is_alive():
                logger.error(
                    "Persist writer pid=%s did not exit in %.1fs; terminating",
                    self._process.pid,
                    timeout,
                )
                self._process.terminate()
                self._process.join(5.0)
        self._closed = True
        close = getattr(self._queue, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                pass
            join_thread = getattr(self._queue, "join_thread", None)
            if callable(join_thread):
                try:
                    join_thread()
                except Exception:
                    pass


def run_trade_persist_writer(
    queue: Any,
    stop_event: Any,
    stats_queue: Any,
    config: TradePersistWriterConfig,
) -> None:
    """Writer process entrypoint. Safe to call from a thread in tests."""
    try:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    except (AttributeError, ValueError):
        pass
    logging.basicConfig(
        level=config.log_level,
        format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    )
    recorder = _build_recorder(config)
    policy = FlushPolicy(
        every_rows=config.every_rows,
        interval_seconds=config.interval_seconds,
    )
    batch: list[dict[str, Any]] = []
    last_append_at = time.monotonic()
    last_compact_at = time.monotonic()

    try:
        recorder.compact()
    except Exception:
        logger.exception("Startup compact of leftover trade parts failed")

    def append_now(*, reason: str) -> None:
        nonlocal batch, last_append_at
        if not batch:
            last_append_at = time.monotonic()
            return
        rows = batch
        batch = []
        started = time.monotonic()
        ok = True
        written = 0
        try:
            summary = recorder.append_rows(rows)
            written = summary.rows_written
        except Exception:
            ok = False
            written = len(rows)
            logger.exception(
                "Append (%s) failed for %d row(s); rows remain only in failed batch",
                reason,
                written,
            )
        duration = time.monotonic() - started
        last_append_at = time.monotonic()
        _emit_stats(
            stats_queue,
            {
                "kind": "flush",
                "rows": written,
                "duration_s": duration,
                "ok": ok,
                "reason": reason,
            },
        )
        if ok:
            logger.info(
                "Append (%s): rows=%d duration_s=%.3f parts=%d",
                reason,
                written,
                duration,
                recorder.part_file_count(),
            )

    def compact_now(*, reason: str) -> None:
        nonlocal last_compact_at
        if recorder.part_file_count() <= 0:
            last_compact_at = time.monotonic()
            return
        started = time.monotonic()
        ok = True
        written = 0
        try:
            summary = recorder.compact()
            written = summary.rows_written
        except Exception:
            ok = False
            logger.exception("Compact (%s) failed", reason)
        duration = time.monotonic() - started
        last_compact_at = time.monotonic()
        _emit_stats(
            stats_queue,
            {
                "kind": "compact",
                "rows": written,
                "duration_s": duration,
                "ok": ok,
                "reason": reason,
            },
        )
        if ok and written:
            logger.info(
                "Compact (%s): rows=%d duration_s=%.3f",
                reason,
                written,
                duration,
            )

    while True:
        stopping = bool(stop_event.is_set())
        try:
            item = queue.get(timeout=_GET_TIMEOUT_S)
        except Empty:
            item = None
        if item is not None:
            batch.append(item)

        if stopping:
            while True:
                try:
                    batch.append(queue.get_nowait())
                except Empty:
                    break
            append_now(reason="shutdown")
            compact_now(reason="shutdown")
            return

        reason = policy.should_flush(
            buffered_rows=len(batch),
            rows_since_flush=len(batch),
            seconds_since_flush=time.monotonic() - last_append_at,
        )
        if reason:
            append_now(reason=reason)

        compact_every = config.compact_interval_seconds
        if (
            compact_every > 0
            and (time.monotonic() - last_compact_at) >= compact_every
        ):
            compact_now(reason=f"every-{compact_every:g}s")


def _build_recorder(config: TradePersistWriterConfig) -> SessionTradeRecorder:
    if config.credentials_path:
        os.environ.setdefault(
            "GOOGLE_APPLICATION_CREDENTIALS", config.credentials_path
        )
    if config.project_id:
        os.environ.setdefault("GOOGLE_CLOUD_PROJECT", config.project_id)
    client = None
    if config.remote_enabled:
        client = storage.Client()
    return SessionTradeRecorder(
        local_root=config.local_root,
        bucket_name=config.bucket_name,
        prefix=config.prefix,
        use_daily_partitions=config.use_daily_partitions,
        remote_enabled=config.remote_enabled,
        client=client,
    )


def _emit_stats(stats_queue: Any, event: dict[str, Any]) -> None:
    if stats_queue is None:
        return
    try:
        stats_queue.put_nowait(event)
    except Exception:
        pass
