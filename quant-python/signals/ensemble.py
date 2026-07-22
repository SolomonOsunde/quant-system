"""
Shared ensemble + trade-level logic for live and backtest.

Single source of truth for weights, direction threshold, confidence fallback,
and ATR-based SL/TP so live paper trading and offline replay cannot drift.
"""
from __future__ import annotations

from typing import Any, Optional


# Live and backtest use the same direction cutoff.
DIRECTION_THRESHOLD = 0.35

# Soft floor for recording / analysis (must still pass DIRECTION_THRESHOLD).
MIN_COMPOSITE = 0.35

BASE_WEIGHTS = {
    "tech":   0.25,
    "ml":     0.20,
    "ofi":    0.20,
    "ts_mom": 0.15,
    "va_mom": 0.05,
    "cs_mom": 0.10,
    "idio":   0.05,
}

# Hold / exit policy shared with live QuantPythonLayer
MIN_HOLD_MINUTES = 30.0
POSITION_MAX_AGE_HOURS = 2.0


def _score_ml(ml_pred: Any) -> tuple[float, float]:
    """Return (signed_score, confidence) from MLPrediction or (dir, conf) tuple."""
    if ml_pred is None:
        return 0.0, 0.0
    if hasattr(ml_pred, "direction") and hasattr(ml_pred, "confidence"):
        return float(ml_pred.direction) * float(ml_pred.confidence), float(ml_pred.confidence)
    if isinstance(ml_pred, (tuple, list)) and len(ml_pred) >= 2:
        d, c = float(ml_pred[0]), float(ml_pred[1])
        return d * c, c
    return 0.0, 0.0


def _score_ofi(ofi: Any) -> float:
    if ofi is None:
        return 0.0
    return float(ofi.direction) * float(ofi.confidence)


def build_ensemble(
    tech_result: Any,
    ml_pred: Any,
    ofi: Any,
    mom_result: Any,
    cs_mom: float,
    idio_mom: float,
    regime: Any,
    *,
    enable_ofi: bool = True,
    enable_ml: bool = True,
    direction_threshold: float = DIRECTION_THRESHOLD,
) -> dict:
    """
    Regime-weighted 7-factor composite.

    Returns dict with direction (+1/-1/0), composite, and component scores.
    Disabled factors (enable_ofi / enable_ml False) have their weight folded
    into tech so the remaining live signals can still clear DIRECTION_THRESHOLD.
    """
    weights = dict(BASE_WEIGHTS)

    if regime is not None:
        w = regime.momentum_weight
        mr = regime.mean_rev_weight
        ofi_w = getattr(regime, "ofi_weight", 0.20)
        total = w + mr + ofi_w + 0.25 + 0.20
        if total > 0:
            weights["tech"]   = 0.25
            weights["ml"]     = 0.20
            weights["ofi"]    = ofi_w / total * 0.55
            weights["ts_mom"] = w / total * 0.55 * 0.6
            weights["va_mom"] = w / total * 0.55 * 0.2
            weights["cs_mom"] = w / total * 0.55 * 0.15
            weights["idio"]   = mr / total * 0.55 * 0.1

    if not enable_ofi:
        weights["tech"] += weights["ofi"]
        weights["ofi"] = 0.0
    if not enable_ml:
        weights["tech"] += weights["ml"]
        weights["ml"] = 0.0

    tech_score = float(tech_result.direction) * float(tech_result.confidence)
    ml_score, _ = _score_ml(ml_pred) if enable_ml else (0.0, 0.0)
    ofi_score = _score_ofi(ofi) if enable_ofi else 0.0
    ts_score = mom_result.ts_momentum if mom_result else 0.0
    va_score = mom_result.vol_adj_momentum if mom_result else 0.0

    composite = (
        weights["tech"]   * tech_score +
        weights["ml"]     * ml_score   +
        weights["ofi"]    * ofi_score  +
        weights["ts_mom"] * ts_score   +
        weights["va_mom"] * va_score   +
        weights["cs_mom"] * cs_mom     +
        weights["idio"]   * idio_mom
    )

    thr = direction_threshold
    direction = 1 if composite > thr else (-1 if composite < -thr else 0)

    return {
        "direction": direction,
        "composite": round(composite, 6),
        "components": {
            "tech": tech_score,
            "ml": ml_score,
            "ofi": ofi_score,
            "ts_mom": ts_score,
            "cs_mom": cs_mom,
        },
    }


def fallback_confidence(
    composite: float,
    tech_conf: float,
    ml_conf: float,
    regime: Any,
    ofi: Any = None,
) -> float:
    """
    Rule-based confidence — mirrors ClaudeReasoningEngine._fallback_decision.

    Does NOT multiply by regime.position_scale (that is applied once to size).
    """
    abs_comp = abs(composite)
    # Scale from soft floor 0.08 up through DIRECTION_THRESHOLD band
    composite_conf = min(max(abs_comp - 0.08, 0.0) / max(DIRECTION_THRESHOLD - 0.08, 0.01), 1.0)
    primary_dir = 1 if composite > 0 else -1

    ofi_bonus = 0.0
    if ofi is not None and getattr(ofi, "confidence", 0) > 0.3:
        if (ofi.direction > 0) == (primary_dir > 0):
            ofi_bonus = 0.06

    ml_contrib = ml_conf if ml_conf > 0 else 0.0

    return round(min(
        0.55 * composite_conf + 0.30 * tech_conf + 0.15 * ml_contrib + ofi_bonus,
        1.0,
    ), 4)


def compute_trade_levels(
    price: float,
    side: str,
    atr_pct: float,
    market: str,
) -> dict:
    """ATR-based SL/TP/leverage — shared by live and backtest."""
    if market == "CRYPTO":
        sl_mult, tp_mult = 1.5, 3.0
        max_lev = 5
    elif market == "FOREX":
        sl_mult, tp_mult = 1.0, 2.0
        max_lev = 10
    else:
        sl_mult, tp_mult = 1.2, 2.4
        max_lev = 3

    atr_dec = max(atr_pct, 0.1) / 100.0
    sl_pct = round(atr_dec * sl_mult * 100, 2)
    tp_pct = round(atr_dec * tp_mult * 100, 2)
    leverage = min(max(round(1.0 / max(atr_dec * sl_mult, 0.005)), 1), max_lev)

    def fmt(p: float) -> float:
        if p >= 1000:
            return round(p, 2)
        if p >= 1:
            return round(p, 4)
        return round(p, 6)

    if side == "BUY":
        sl = fmt(price * (1 - atr_dec * sl_mult))
        tp = fmt(price * (1 + atr_dec * tp_mult))
    else:
        sl = fmt(price * (1 + atr_dec * sl_mult))
        tp = fmt(price * (1 - atr_dec * tp_mult))

    return {
        "entry": fmt(price),
        "stop_loss": sl,
        "take_profit": tp,
        "sl_pct": sl_pct,
        "tp_pct": tp_pct,
        "leverage": leverage,
    }


def path_exit(
    side: str,
    entry_bar: int,
    highs: Any,
    lows: Any,
    closes: Any,
    stop_loss: float,
    take_profit: float,
    *,
    min_hold_bars: int = 30,
    max_hold_bars: int = 120,
) -> tuple[int, float, str]:
    """
    Walk bars after entry; enforce min-hold then SL/TP using bar H/L.

    If both SL and TP are touched in the same bar, assume the level closer
    to the prior close was hit first (path ambiguity).
    Returns (exit_bar_index, exit_price, reason).
    """
    n = len(closes)
    last = min(entry_bar + max_hold_bars, n - 1)

    for i in range(entry_bar, last + 1):
        held = i - entry_bar
        if held >= max_hold_bars:
            return i, float(closes[i]), "TIME_STOP"

        if held < min_hold_bars:
            continue

        hi = float(highs[i])
        lo = float(lows[i])
        prev = float(closes[i - 1]) if i > 0 else float(closes[i])

        hit_sl = hit_tp = False
        if side == "BUY":
            hit_sl = lo <= stop_loss
            hit_tp = hi >= take_profit
        else:
            hit_sl = hi >= stop_loss
            hit_tp = lo <= take_profit

        if hit_sl and hit_tp:
            # Ambiguous bar — closer level to prior close hit first
            if abs(prev - stop_loss) <= abs(prev - take_profit):
                return i, stop_loss, "STOP_LOSS"
            return i, take_profit, "TAKE_PROFIT"
        if hit_sl:
            return i, stop_loss, "STOP_LOSS"
        if hit_tp:
            return i, take_profit, "TAKE_PROFIT"

    return last, float(closes[last]), "TIME_STOP"
