# Pipeline Modules

Every production module (excluding `test_*.py` files), organized by where it sits in the pipeline. The live trading data flow is the spine; supporting layers are grouped after it.

## Live trading pipeline (the main flow)

| #   | Module                         | Role in the pipeline                                                                  |
| --- | ------------------------------ | ------------------------------------------------------------------------------------- |
| 1   | `config.py`                    | Loads all non-secret settings from `config.json` into a typed `AppConfig`.            |
| 2   | `schwab_auth.py`               | Refreshes OAuth access tokens from local/GCS token stores for every Schwab call.      |
| 3   | `schwab_streamer.py`           | Opens the Schwab WebSocket, logs in, and subscribes to `CHART_EQUITY` for 1m bars.    |
| 4   | `stream_connection_manager.py` | Manages WebSocket lifecycle (connect, reconnect, heartbeat) for the generic provider. |
| 5   | `stream_data_processor.py`     | Validates/dedupes raw 1m bars into `CleanBarEvent`s and emits them.                   |
| 6   | `ohlc_sanity.py`               | Repairs malformed/outlier OHLC values on streamed and stored bars.                    |
| 7   | `bar_alignment.py`             | Floors all timestamps to UTC bucket boundaries so aggregation/backfill agree.         |
| 8   | `data_aggregator.py`           | Rolls 1m bars up into strategy timeframes (e.g. 3m) with incomplete-bar buffers.      |
| 9   | `indicator_calculator.py`      | Stateless math for DEMA/Supertrend/RSI/MACD/SMA/EMA.                                  |
| 10  | `indicator_coordinator.py`     | Holds bar buffers + indicator config and dispatches jobs to produce snapshots.        |
| 11  | `strategy_registry.py`         | Stores the strategy rule definitions and required indicators.                         |
| 12  | `signal_evaluator.py`          | Turns indicator snapshots + rules into BUY/SELL/HOLD signals.                         |
| 13  | `position_sizer.py`            | Converts balance + price into share/contract order quantities.                        |
| 14  | `option_selector.py`           | Resolves the ATM call/put OCC symbol and mark at the target DTE.                      |
| 15  | `option_quote.py`              | Bid/ask + Greeks snapshot attached to each option trade record.                       |
| 16  | `position_reconciliation.py`   | Checks a restored option still matches current Supertrend direction.                  |
| 17  | `workflow.py`                  | Orchestrates the whole pipeline + `RiskGuard` pre-trade checks via the event bus.     |
| 18  | `order_manager.py`             | Submits orders to a broker gateway and tracks execution.                              |
| 19  | `position_tracker.py`          | Tracks live positions, P&L, stops/targets, and exit alerts.                           |

## Broker / account (execution backend)

| #   | Module                     | Role in the pipeline                                          |
| --- | -------------------------- | ------------------------------------------------------------- |
| 20  | `schwab_order_builder.py`  | Converts internal orders into Schwab order JSON.              |
| 21  | `schwab_broker_gateway.py` | Places, polls, previews, cancels, and lists Schwab orders.    |
| 22  | `schwab_trader_client.py`  | Accounts, positions, orders, and `userPreference` REST calls. |
| 23  | `schwab_account_sync.py`   | Syncs broker balances/positions into the `PositionTracker`.   |

## Market data normalization (shared)

| #   | Module                         | Role in the pipeline                                                 |
| --- | ------------------------------ | -------------------------------------------------------------------- |
| 24  | `ohlcv_schema.py`              | Canonical OHLCV column definition + coercion.                        |
| 25  | `market_data_transformer.py`   | Vendor payload → standard OHLCV.                                     |
| 26  | `market_data_api_client.py`    | Generic HTTP transport (auth, rate limits, retries) for market data. |
| 27  | `schwab_market_data_client.py` | `GET /pricehistory` with date-range chunking + option chains.        |

## Historical / backfill pipeline (REST)

| #   | Module                       | Role in the pipeline                                                       |
| --- | ---------------------------- | -------------------------------------------------------------------------- |
| 28  | `gap_detector.py`            | Finds missing dates/intervals against the expected timeline.               |
| 29  | `backfill_executor.py`       | Fetches, normalizes, and persists planned backfills.                       |
| 30  | `historical_orchestrator.py` | Plans and runs the full historical sync workflow.                          |
| 31  | `workflow_warmup.py`         | Backfills + replays recent bars to warm indicator state before going live. |

## Persistence

| #   | Module                        | Role in the pipeline                                                |
| --- | ----------------------------- | ------------------------------------------------------------------- |
| 32  | `cloud_storage_repository.py` | GCS read/write of standard OHLCV Parquet.                           |
| 33  | `local_storage_repository.py` | Local disk mirror of the GCS layout, optionally replicating to GCS. |
| 34  | `session_ohlcv_recorder.py`   | Buffers live session bars and flushes them to storage on shutdown.  |
| 35  | `transaction_ledger.py`       | Append-only CSV of every entry/exit (equity + options).             |

## Forward-test / notifications

| #   | Module                        | Role in the pipeline                                                     |
| --- | ----------------------------- | ------------------------------------------------------------------------ |
| 36  | `forward_test_account.py`     | Paper account tracking cash, realized P&L, and open positions.           |
| 37  | `emailer.py`                  | Sends buy/sell alert emails in forward-test mode instead of real orders. |
| 38  | `market_session_scheduler.py` | UTC end-of-day flatten/shutdown scheduling helpers.                      |

## Cross-cutting infrastructure

| #   | Module              | Role in the pipeline                                            |
| --- | ------------------- | --------------------------------------------------------------- |
| 39  | `event_bus.py`      | In-process pub/sub backbone connecting all producers/consumers. |
| 40  | `trade_logger.py`   | Durable JSON-lines audit trail of signals, orders, and fills.   |
| 41  | `health_monitor.py` | Watches feed latency, reconnects, and silent modules.           |

## OAuth setup (one-time / weekly)

| #   | Module                | Role in the pipeline                                           |
| --- | --------------------- | -------------------------------------------------------------- |
| 42  | `schwab_oauth.py`     | Runs the local browser OAuth flow and captures the callback.   |
| 43  | `authorize_schwab.py` | CLI entrypoint that performs OAuth then uploads tokens to GCS. |

## Notes

- The `#` reflects roughly where a module enters the flow (ingest → process → strategy → execute → record), not a strict runtime sequence.
- The true backbone is `event_bus.py` (#39); `workflow.py` (#17) is the orchestrator that wires everything together.
- `trade_logger` and `health_monitor` subscribe passively rather than sitting inline.
- The historical/backfill group (28–31) runs at startup (warmup) and offline, feeding storage that the live pipeline reads from.
