"""Human-readable status lines for gex_scalp console feedback."""

from __future__ import annotations

from strategy_registry import SignalAction, StrategyEvaluationContext


def describe_gex_scalp_status(
    ctx: StrategyEvaluationContext,
    *,
    action: SignalAction,
) -> str:
    """Return a concise status string explaining the current gex_scalp state."""
    gex = ctx.gex
    if gex is None:
        return "waiting for GEX snapshot (options chain poll in progress)"

    state = ctx.state
    volume_sma = ctx.indicators.get("volume_sma")
    volume_mult = float(ctx.indicators.get("gex_volume_multiplier", 1.5))
    put_wall_break_pct = float(ctx.indicators.get("gex_put_wall_break_pct", 0.001))

    if ctx.has_open_position:
        return _describe_open_position(ctx, state, action)

    if gex.regime != "negative":
        return (
            f"need negative GEX regime (current={gex.regime}, "
            f"net={gex.net_gex:,.0f})"
        )

    volume_spike, volume_detail = _volume_spike_detail(
        ctx.volume,
        volume_sma,
        volume_mult,
    )
    if not volume_spike:
        return f"need volume spike — {volume_detail}"

    parts = [f"armed: negative regime, volume ok ({volume_detail})"]
    if gex.put_wall is not None:
        threshold = gex.put_wall * (1.0 - put_wall_break_pct)
        parts.append(
            f"put_wall break: close {ctx.close:.2f} need < {threshold:.2f} "
            f"(wall={gex.put_wall:.2f})"
        )
    if gex.flip_level is not None:
        parts.append(
            f"flip magnet @ {gex.flip_level:.2f} "
            f"(cross up→call, cross down→put)"
        )
    if action != SignalAction.HOLD:
        return " | ".join(parts) + f" → {action.value.upper()}"
    return " | ".join(parts)


def _volume_spike_detail(
    volume: float,
    volume_sma: object,
    volume_mult: float,
) -> tuple[bool, str]:
    if volume_sma is None:
        return False, "volume SMA warming up"
    sma = float(volume_sma)
    if sma <= 0:
        return False, "volume SMA warming up"
    need = sma * volume_mult
    ok = volume >= need
    detail = f"vol={volume:,.0f} need>={need:,.0f} (sma={sma:,.0f} x{volume_mult:g})"
    return ok, detail


def _describe_open_position(
    ctx: StrategyEvaluationContext,
    state: dict[str, object],
    action: SignalAction,
) -> str:
    trigger = state.get("trigger_level")
    entry_side = state.get("entry_side")
    if trigger is None or entry_side not in {"call", "put"}:
        return "in position (exit rules warming up)"

    trigger_f = float(trigger)
    stop_dir = ">" if entry_side == "put" else "<"
    consecutive = int(state.get("consecutive_directional", 0))
    base = (
        f"managing {entry_side} | trigger={trigger_f:.2f} close={ctx.close:.2f} "
        f"stop if close {stop_dir} {trigger_f:.2f} | consec_bars={consecutive}/3"
    )
    if action == SignalAction.EXIT:
        return base + " → EXIT"
    return base
