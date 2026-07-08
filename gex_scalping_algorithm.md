# GEX-Based 0DTE Scalping Algorithm

Integration spec: mapping the Negative-GEX scalping strategy onto the existing
pipeline architecture. Reuses modules where possible, adds only what's missing.

> **Risk note:** This strategy trades 0DTE ATM options based on inferred dealer
> positioning (a standard industry assumption, not observed fact). It is fast,
> leveraged, and assumption-dependent. Nothing below is a recommendation to
> trade this way — treat position sizing, max-loss-per-day, and kill-switch
> logic as first-class requirements, not afterthoughts. Backtest and paper-trade
> before running live.

---

## 1. What's new vs. what's reused

Your existing pipeline handles **equity/index OHLCV bars**. This strategy needs
**options chain data** (OI, strikes, IV) as a second data source that feeds a
new regime layer sitting *alongside* your indicator pipeline, not inside it.

| Layer | Existing module | Reused as-is? |
|---|---|---|
| Broker auth | `schwab_auth`, `schwab_oauth` | ✅ yes |
| Live equity bars | `schwab_streamer` → `stream_data_processor` | ✅ yes (for spot price + entry trigger candles) |
| Order execution | `order_manager`, `schwab_order_builder`, `schwab_broker_gateway` | ✅ yes |
| Position/PnL | `position_tracker` | ✅ yes, extend for 0DTE options (see §5) |
| Event backbone | `event_bus` | ✅ yes |
| Audit | `trade_logger`, `health_monitor` | ✅ yes |
| Bar rollups | `data_aggregator` | ⚠️ optional — this strategy is 1m-native, you may not need 5m/1h rollups at all |
| Indicators (DEMA/RSI/etc.) | `indicator_calculator`, `indicator_coordinator` | ⚠️ not central to this strategy — GEX levels replace technical indicators as the primary signal |
| Strategy rules | `strategy_registry`, `signal_evaluator` | ✅ reused, but rules are GEX-based (see §4) instead of indicator-based |

**New modules needed:**

| Module | Responsibility | Does not |
|---|---|---|
| `schwab_options_chain_client` | `GET /chains` — pulls strikes, OI, IV per expiration | Greeks math, GEX math, normalization |
| `options_chain_transformer` | Vendor chain payload → standard per-strike schema (strike, type, OI, IV, expiry) | HTTP, storage, greeks |
| `greeks_calculator` | Stateless Black-Scholes gamma (and delta, for strike selection) per contract | Config, dispatch, chain fetching |
| `gex_calculator` | Aggregates per-strike GEX, computes Net GEX, Zero-Gamma Flip level, Put Wall, Call Wall | Fetching, greeks math, order logic |
| `gex_regime_monitor` | Polls chain on interval, republishes `gex.snapshot` to event bus, tracks regime transitions | Storage, greeks math, order logic |
| `zero_dte_contract_selector` | Given spot + Net GEX signal, picks the actual ATM/1-OTM 0DTE contract to trade (symbol, strike, delta check) | Order submission, PnL |

---

## 2. Data flow

### 2a. GEX snapshot pipeline (new — runs in parallel to your live bar pipeline)

```
schwab_options_chain_client.fetch_chain(symbol, expiry=0DTE)
        ↓ raw vendor JSON (strikes, OI, IV)
options_chain_transformer.normalize()
        ↓ standard per-strike rows
greeks_calculator.compute(spot, strike, T, r, iv)
        ↓ gamma per contract
gex_calculator.aggregate()
        ↓ { net_gex, flip_level, put_wall, call_wall, per_strike_gex[] }
gex_regime_monitor.publish("gex.snapshot")
        ↓
EventBus
```

Poll interval: chain data doesn't need 1-second refresh — every 15–30s is
plenty, since OI barely moves intraday. Don't hammer the endpoint at 1m-bar
speed; it buys you nothing and burns rate limit budget.

### 2b. Live trading pipeline (existing, with a new consumer)

```
SchwabStreamSession → CHART_EQUITY (1m bars)
        ↓
StreamDataProcessor → "bar.clean"
        ↓
EventBus ─────────────┬─────────────────────────────
                       │                             │
                       ▼                             ▼
        signal_evaluator (GEX rules,          data_aggregator (optional,
        subscribes to bar.clean               only if you still want
        AND gex.snapshot)                     5m/1h context filters)
                       ↓ "strategy.signal"
        zero_dte_contract_selector
                       ↓ contract + side
        RiskGuard (size/day-loss checks)
                       ↓
        OrderManager → SchwabBrokerGateway
                       ↓ "order.fill"
        PositionTracker (0DTE-aware: theta/IV-crush aware exit clock)
                       ↓
EventBus subscribers: TradeLogger, HealthMonitor
```

Key point: `signal_evaluator` now needs **two** input streams (bar data +
GEX snapshots) instead of one. That's the main architectural change — it's a
dual-subscriber pattern, not a rewrite.

---

## 3. `gex_calculator` — core math (module spec)

Stateless, same style as `indicator_calculator`.

```python
def call_gex(gamma, open_interest, spot, multiplier=100):
    return gamma * open_interest * multiplier * spot

def put_gex(gamma, open_interest, spot, multiplier=100):
    return -gamma * open_interest * multiplier * spot

def net_gex(strikes: list[StrikeRow]) -> float:
    return sum(
        call_gex(s.gamma, s.oi, s.spot) if s.type == "call"
        else put_gex(s.gamma, s.oi, s.spot)
        for s in strikes
    )

def find_put_wall(strikes: list[StrikeRow]) -> float:
    # strike with the largest-magnitude negative GEX
    puts = [s for s in strikes if s.type == "put"]
    return min(puts, key=lambda s: s.gex).strike

def find_zero_gamma_flip(strikes: list[StrikeRow]) -> float:
    # strike where cumulative GEX crosses from + to - as you scan by price
    ...
```

Output object published on `gex.snapshot`:

```json
{
  "symbol": "SPY",
  "timestamp": "...",
  "net_gex": -1250000000,
  "regime": "negative",
  "flip_level": 538.50,
  "put_wall": 535.00,
  "call_wall": 542.00
}
```

---

## 4. Strategy rules (→ `strategy_registry` entries, evaluated by `signal_evaluator`)

Translating the playbook directly into rule form:

| Rule | Condition | Action |
|---|---|---|
| **Regime filter** | `gex.snapshot.regime == "negative"` (spot below flip_level) | Arm the strategy; otherwise stand down |
| **Trigger A — Put Wall break** | 1m candle closes >X% below `put_wall` on volume > N× average | BUY PUT |
| **Trigger B — Magnet snap** | 1m candle breaks a local minor strike level on volume spike, regime negative | BUY CALL or PUT in breakout direction |
| **Contract selection** | via `zero_dte_contract_selector`: pick 0DTE, delta 45–50, ATM or 1-OTM | — |
| **Take profit** | 1m candle stalls / reverses color / long wick, OR 3rd consecutive directional candle closes | SELL 100% |
| **Stop loss** | Next 1m candle closes back on the wrong side of the trigger level | Market-sell immediately, no averaging down |

These conditions belong in `strategy_registry` as declarative rules (same
pattern you already use), evaluated by `signal_evaluator` — just extend the
evaluator's input schema to include the latest `gex.snapshot` alongside bars.

**Gaps the playbook doesn't specify — you should decide these before going live:**
- Max concurrent positions / max trades per day
- Daily max-loss kill switch (this belongs in a `RiskGuard`-type check before `OrderManager`, sitting inline in §2b)
- What "volume > N× average" actually resolves to numerically
- Slippage/spread filter — 0DTE ATM spreads can widen fast during the exact volatility bursts you're trying to trade

---

## 5. `position_tracker` extension

0DTE options decay differently from equities — add:
- Entry timestamp + max hold timer (this strategy expects exits within ~2-4 minutes; a stale open position past ~10 min should force-flag for manual review)
- IV-at-entry vs IV-at-mark, since your stop/TP logic depends on premium change, not just spot change

---

## 6. Build order (suggested)

1. `schwab_options_chain_client` + `options_chain_transformer` — get real chain data flowing
2. `greeks_calculator` — validate gamma numbers against a known reference (e.g. compare to a paid GEX data provider for a day) before trusting your own math
3. `gex_calculator` + `gex_regime_monitor` — publish `gex.snapshot`, log it for a few days *without trading* to sanity-check flip levels/put walls against what you'd eyeball manually
4. Extend `signal_evaluator` to consume both streams; wire up rules in `strategy_registry`
5. `zero_dte_contract_selector`
6. Paper-trade the full loop before connecting `OrderManager` to a funded account

Steps 3 and 6 are the ones worth not rushing — bad gamma math or an unvalidated
signal path is much more expensive to discover live than in a log file.
