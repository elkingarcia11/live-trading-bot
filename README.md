# Live Trading Bot

Python utilities for a live trading bot with a strict separation of concerns across market data modules.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Fill SCHWAB_APP_KEY, SCHWAB_APP_SECRET, GMAIL_APP_PASSWORD, GOOGLE_APPLICATION_CREDENTIALS
```

### Schwab OAuth (local → GCS)

Schwab requires a browser login about once a week. The bot refreshes the
short-lived access token automatically from the stored refresh token.

**1. Register the callback URL** in the [Schwab developer portal](https://developer.schwab.com).
Default in `config.json`:

```json
"callback_url": "https://127.0.0.1",
"callback_port": 443
```

The redirect URI must match exactly. Port `443` on macOS/Linux often requires
`sudo` for the callback listener. You can instead use e.g.
`http://127.0.0.1:8182` if that URI is registered in your Schwab app (update
`callback_url` and `callback_port` in `config.json`).

**2. Configure token storage** in `config.json`:

```json
"gcs": {
  "bucket_name": "live-trading-bot",
  "schwab_token_path": "schwab/tokens.json"
},
"schwab": {
  "token_file": ".schwab_tokens.json"
}
```

**3. Run the local OAuth helper** (needs `GOOGLE_APPLICATION_CREDENTIALS` or
`gcloud auth application-default login` for the GCS upload):

```bash
# https://127.0.0.1:443 often needs elevated privileges for the callback server
sudo .venv/bin/python authorize_schwab.py

# Or without sudo when using http://127.0.0.1:8182 in config.json
.venv/bin/python authorize_schwab.py
```

This will:

1. Open the Schwab authorize URL in your browser (or print it with `--no-browser`)
2. Capture the redirect on your local callback URL
3. Exchange the code for access + refresh tokens
4. Save tokens to **`.schwab_tokens.json`** (local)
5. Upload the same JSON to **`gs://{bucket}/schwab/tokens.json`** (cloud)

**4. Cloud VM usage:** `workflow.py` and `SchwabAuthClient.from_env()` load
tokens from the local file first, then from GCS. On a VM with no local token
file, the bot reads `gs://live-trading-bot/schwab/tokens.json` automatically.
Refreshed access tokens are written back to every configured store.

Re-run `authorize_schwab.py` locally when the refresh token expires (~7 days)
or Schwab returns an auth error on the VM.

**Manual fallback:** set `SCHWAB_REFRESH_TOKEN` in `.env` or Secret Manager
instead of using GCS token storage.

## Architecture

Each module owns one layer of the pipeline. Downstream modules consume outputs from upstream modules, but responsibilities do not overlap.

| Module                      | Responsibility                                                           | Does not                                       |
| --------------------------- | ------------------------------------------------------------------------ | ---------------------------------------------- |
| `ohlcv_schema`              | Canonical OHLCV column definition and coercion for already-standard data | Vendor mapping, I/O, networking                |
| `market_data_transformer`   | Vendor payload → standard OHLCV                                          | HTTP, WebSockets, persistence, live validation |
| `market_data_api_client`    | HTTP auth, rate limits, retries, raw JSON responses                      | Data normalization, storage, streaming         |
| `stream_connection_manager` | WebSocket lifecycle, reconnect, heartbeats                               | Message parsing, bar validation, storage       |
| `stream_data_processor`     | Live 1m bar validation, deduplication, event publishing                  | WebSocket I/O, vendor mapping, storage         |
| `cloud_storage_repository`  | GCS read/write for standard OHLCV Parquet                                | Remote fetching, vendor mapping, streaming     |
| `gap_detector`              | Missing date/interval detection against a timeline                       | Storage, HTTP, vendor mapping                  |
| `backfill_executor`         | Fetch, normalize, and persist planned backfills                          | Gap detection, workflow planning               |
| `historical_orchestrator`   | Plan and run historical sync workflows                                   | Direct HTTP, vendor mapping, gap algorithms    |
| `order_manager`             | Broker order submission and execution tracking                           | PnL, stops, portfolio state                    |
| `position_tracker`          | Live positions, PnL, stops/targets, exit alerts                          | Broker order submission                        |
| `data_aggregator`           | Roll 1m bars into 5m/1h/1d with incomplete bar buffers                   | Indicators, strategies, storage                |
| `indicator_calculator`      | Stateless DEMA/Supertrend/RSI/MACD/SMA/EMA math                          | Config, dispatch, strategy rules               |
| `indicator_coordinator`     | Indicator config, bar buffers, job dispatch                              | Indicator formulas, signal rules               |
| `strategy_registry`         | Store strategy rule definitions                                          | Live evaluation, indicator math                |
| `signal_evaluator`          | Evaluate rules → BUY/SELL/HOLD                                           | Indicators, aggregation, broker orders         |
| `event_bus`                 | In-process pub/sub backbone                                              | Persistence, health logic, audit writes        |
| `trade_logger`              | Durable audit trail for signals, orders, fills                           | Event routing, strategy evaluation             |
| `health_monitor`            | Feed latency, reconnects, module silence alerts                          | Trade execution, audit persistence             |
| `workflow`                  | Orchestrates the full live pipeline via the event bus                    | Low-level module internals                     |
| `schwab_auth`               | OAuth token refresh, local/GCS token stores, `.env` loading                | Market data, streaming, trading                |
| `schwab_oauth`              | Local browser OAuth flow and callback capture                            | Live trading, order submission                 |
| `authorize_schwab.py`       | CLI entrypoint for local OAuth → GCS token upload                        | Strategy, streaming                            |
| `schwab_market_data_client` | `GET /pricehistory` with date-range chunking                             | Normalization, storage                         |
| `schwab_trader_client`      | Accounts, positions, orders, `userPreference`                            | WebSocket sessions                             |
| `schwab_streamer`           | WebSocket LOGIN + `CHART_EQUITY` → 1m bars                               | Strategy, storage                              |
| `schwab_account_sync`       | Sync broker positions into `PositionTracker`                             | Order submission                               |
| `schwab_order_builder`      | Internal orders → Schwab order JSON                                      | HTTP transport                                 |
| `schwab_broker_gateway`     | Place, poll, cancel, preview, list orders                                | Signal evaluation                              |

### Data flow

**Historical (REST)**

```
HistoricalOrchestrator.plan()
        ↓ inspects CloudStorageRepository
GapDetector.analyze()
        ↓ missing dates / intervals
BackfillExecutor.execute_many()
        ↓
MarketDataApiClient.fetch_paginated()
        ↓ raw vendor JSON
MarketDataTransformer.from_bars()
        ↓ standard OHLCV DataFrame
CloudStorageRepository.write()
```

**Live (WebSocket — Schwab default)**

```
GET /userPreference → streamerSocketUrl + client IDs
SchwabStreamSession → WebSocket ADMIN LOGIN → CHART_EQUITY SUBS
        ↓ 1m OHLCV bars
StreamDataProcessor → CleanBarEvent
        ↓ publish bar.clean
DataAggregator → IndicatorCoordinator → SignalEvaluator
        ↓ bar.aggregated / indicators.snapshot / strategy.signal
RiskGuard → OrderManager → SchwabBrokerGateway (or in-memory broker)
        ↓ order.updated / order.fill
PositionTracker
EventBus subscribers: TradeLogger, HealthMonitor
```

**Live (generic WebSocket)**

```
StreamConnectionManager → StreamDataProcessor → (same pipeline as above)
```

Use the event bus instead of wiring every consumer directly into the stream
processor. Producers publish; passive listeners subscribe.

### OHLCV schema

Defined once in `ohlcv_schema.py`:

| Column    | Type     | Notes |
| --------- | -------- | ----- |
| timestamp | datetime | UTC   |
| open      | numeric  |       |
| high      | numeric  |       |
| low       | numeric  |       |
| close     | numeric  |       |
| volume    | numeric  |       |

## Module examples

### Historical pipeline

```python
from datetime import datetime, timezone

from cloud_storage_repository import CloudStorageRepository
from historical_orchestrator import HistoricalOrchestrator
from schwab_market_data_client import build_schwab_backfill_executor

storage = CloudStorageRepository("my-trading-bucket")
executor = build_schwab_backfill_executor(storage)
orchestrator = HistoricalOrchestrator(storage, executor)

plan, results = orchestrator.run(
    "AAPL",
    "1m",
    datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc),
    datetime(2024, 1, 19, 16, 0, tzinfo=timezone.utc),
)

print(f"Bootstrapped from earliest: {plan.bootstrapped_from_earliest}")
print(f"Backfill requests: {len(plan.backfill_requests)}")
print(f"Rows written: {sum(result.rows_written for result in results)}")
```

When storage is empty for a symbol, the orchestrator discovers the provider's
earliest available history and plans backfills from that point through the
requested end date (`bootstrap_if_empty=True` by default).

### Schwab live stream

Requires valid OAuth tokens and Market Data Production enabled in the Schwab
developer portal.

```python
from schwab_streamer import SchwabStreamSession, build_schwab_stream_processor
from stream_data_processor import CleanBarEvent

def on_bar(event: CleanBarEvent) -> None:
    print(event.to_dict())

processor = build_schwab_stream_processor(
    symbols=("SPY", "QQQ", "TSLA", "AMZN", "NVDA"),
    consumers=[on_bar],
)
session = SchwabStreamSession.from_env(symbols=processor.symbols, processor=processor)
session.refresh_streamer_info()
session.connect()
# session.disconnect() when done
```

Or run the full workflow:

```bash
RUN_SCHWAB_STREAM=true .venv/bin/python workflow.py
```

Streamer flow (see `Schwab_Streamer_Documentation.md`):

1. `GET /trader/v1/userPreference` — streamer URL and client metadata
2. WebSocket `ADMIN LOGIN` with access token (no `Bearer` prefix)
3. `CHART_EQUITY SUBS` with `fields=0,1,2,3,4,5,6,7` for 1-minute candles

### Generic live pipeline

```python
from stream_connection_manager import StreamConnectionManager
from stream_data_processor import CleanBarEvent, StreamDataProcessor

def on_clean_bar(event: CleanBarEvent) -> None:
    print(event.to_dict())

processor = StreamDataProcessor(symbols=("SPY", "QQQ", "TSLA"), consumers=[on_clean_bar])

manager = StreamConnectionManager(
    "wss://stream.example.com/v1",
    on_message=processor.process_message,
)
manager.connect()
```

### Execution and risk

**Simulated broker (default, safe for development)**

```python
from datetime import datetime, timezone

from order_manager import InMemoryBrokerGateway, OrderManager, OrderSide, TradingSignal
from position_tracker import PositionTracker

tracker = PositionTracker(exit_handlers=[lambda n: print(n.to_dict())])
broker = InMemoryBrokerGateway(fill_price=185.25)

def on_order_update(order):
    fill = manager.to_fill_event(order)
    if fill is not None:
        tracker.on_fill(fill)

manager = OrderManager(broker, on_update=on_order_update)
manager.submit_signal(TradingSignal(symbol="AAPL", side=OrderSide.BUY, quantity=10))

tracker.update_price("AAPL", 183.9, timestamp=datetime.now(timezone.utc))
```

**Schwab live broker**

```python
from schwab_broker_gateway import SchwabBrokerGateway
from schwab_account_sync import SchwabAccountSync

# BROKER_USE_IN_MEMORY=false in .env
gateway = SchwabBrokerGateway.from_env()
sync = SchwabAccountSync.from_env()
sync.sync_positions(tracker, watchlist=("SPY", "QQQ"))

# Optional dry-run: SCHWAB_PREVIEW_ORDERS=true validates via POST /previewOrder
```

| Schwab Trader API                        | Module                                     |
| ---------------------------------------- | ------------------------------------------ |
| `GET /accounts/accountNumbers`           | `schwab_trader_client`                     |
| `GET /accounts` / `GET /accounts/{hash}` | `schwab_trader_client`                     |
| `GET /userPreference`                    | `schwab_trader_client` → `schwab_streamer` |
| `POST /accounts/{hash}/previewOrder`     | `schwab_broker_gateway`                    |
| `POST /accounts/{hash}/orders`           | `schwab_broker_gateway`                    |
| `GET /accounts/{hash}/orders/{id}`       | `schwab_broker_gateway`                    |
| `GET /accounts/{hash}/orders`            | `schwab_broker_gateway`                    |
| `DELETE /accounts/{hash}/orders/{id}`    | `schwab_broker_gateway`                    |
| `GET /marketdata/v1/pricehistory`        | `schwab_market_data_client`                |

`{hash}` is the encrypted account id from `accountNumbers`, not the plain
account number.

### Strategy pipeline

```
CleanBarEvent (1m)
    → DataAggregator → AggregatedBar (5m/1h/1d)
    → IndicatorCoordinator → indicator values
    → SignalEvaluator + StrategyRegistry → BUY/SELL/HOLD
```

```python
from datetime import datetime, timedelta, timezone

from data_aggregator import DataAggregator
from indicator_coordinator import (
    IndicatorCoordinator,
    SymbolIndicatorConfig,
    build_dema_job,
    build_supertrend_job,
)
from signal_evaluator import SignalEvaluator
from strategy_registry import build_default_registry
from stream_data_processor import CleanBarEvent

coordinator = IndicatorCoordinator()
coordinator.register(
    SymbolIndicatorConfig(
        symbol="AAPL",
        jobs=(
            build_dema_job("5m", period=200, source="close"),
            build_supertrend_job("5m", atr_period=12, source="hl2", multiplier=3.0),
        ),
    )
)

aggregator = DataAggregator()
evaluator = SignalEvaluator(build_default_registry())

bar = CleanBarEvent("AAPL", "1m", datetime.now(timezone.utc), 185, 186, 184, 185.5, 1000)
for aggregated in aggregator.on_bar(bar):
    snapshot = coordinator.on_aggregated_bar(aggregated)
    if snapshot and aggregated.is_complete:
        signal = evaluator.evaluate(
            symbol="AAPL",
            timeframe="5m",
            timestamp=aggregated.timestamp,
            close=aggregated.close,
            indicators=snapshot.values,
            strategy_name="dema_trend",
        )
        print(signal.to_dict())
```

### Full live workflow

`workflow.py` wires ingest → process → strategy → execute with the event bus as
the backbone and passive audit/health listeners.

**Run modes**

| Command                                     | What it does                                                |
| ------------------------------------------- | ----------------------------------------------------------- |
| `python workflow.py`                        | Offline simulation — replays synthetic 1m bars (no network) |
| `RUN_SCHWAB_STREAM=true python workflow.py` | Live Schwab `CHART_EQUITY` WebSocket feed                   |
| `BROKER_USE_IN_MEMORY=false`                | Routes orders to Schwab (use with preview mode first)       |

```python
from workflow import DEFAULT_SYMBOLS, TradingWorkflow, WorkflowConfig

# Schwab live stream (default STREAM_PROVIDER=schwab in .env)
workflow = TradingWorkflow(WorkflowConfig.from_env())
workflow.start()
# workflow.stop()

# Generic WebSocket provider
workflow = TradingWorkflow(
    WorkflowConfig(
        websocket_url="wss://stream.example.com/v1",
        stream_provider="generic",
        symbols=DEFAULT_SYMBOLS,
        strategies=("dema_trend",),
        dema_period=200,
        dema_source="close",
        supertrend_atr_period=12,
        supertrend_source="hl2",
        supertrend_multiplier=3.0,
        order_quantity=10,
        stop_loss=180.0,
        take_profit=190.0,
    )
)

# Or simulate bars without a socket:
# workflow.process_clean_bar(clean_bar_event)
```

Key `.env` groups: secrets only (`SCHWAB_*`, `GMAIL_APP_PASSWORD`, `GOOGLE_*`).
All other settings live in `config.json`. See `.env.example` for the full list.

### Infrastructure (pub/sub, audit, observability)

```python
from datetime import datetime, timezone

from event_bus import EventBus, Topics
from health_monitor import HealthMonitor
from stream_data_processor import CleanBarEvent, StreamDataProcessor
from trade_logger import TradeLogger

bus = EventBus()
trade_logger = TradeLogger(bus, log_path="logs/audit.jsonl")
health_monitor = HealthMonitor(bus)
trade_logger.start()
health_monitor.start()

def on_clean_bar(event: CleanBarEvent) -> None:
    bus.publish(Topics.BAR_CLEAN, event, source="stream_data_processor")

processor = StreamDataProcessor(symbols=("SPY", "QQQ", "TSLA"), consumers=[on_clean_bar])

# Producers publish with a `source` so HealthMonitor can detect silent modules.
bus.publish(
    Topics.STREAM_RECONNECTING,
    {"url": "wss://stream.example.com/v1"},
    source="stream_connection_manager",
)

snapshot = health_monitor.check()
print(snapshot.to_dict())
```

Canonical topics live in `event_bus.Topics`. Important audit topics:

| Topic             | Payload              |
| ----------------- | -------------------- |
| `strategy.signal` | `StrategySignal`     |
| `risk.decision`   | `RiskDecisionRecord` |
| `order.updated`   | `Order`              |
| `order.fill`      | `FillEvent`          |
| `position.exit`   | `ExitNotification`   |
| `bar.clean`       | `CleanBarEvent`      |

`TradeLogger` writes append-only JSON Lines to durable local storage. Swap the
bus implementation later (Redis Streams, NATS, etc.) without changing producers.

### Cloud Storage

Requires [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials):

- **Local:** `gcloud auth application-default login`
- **Production:** set `GOOGLE_APPLICATION_CREDENTIALS`

Storage layout:

```
gs://{bucket}/ohlcv/{SYMBOL}/{TIMEFRAME}/data.parquet
gs://{bucket}/ohlcv/{SYMBOL}/{TIMEFRAME}/{YYYY-MM-DD}.parquet
gs://{bucket}/schwab/tokens.json
gs://{bucket}/forward_test/account.json
```

`CloudStorageRepository` expects data that already uses the standard OHLCV schema. Normalize vendor payloads with `MarketDataTransformer` before writing.
