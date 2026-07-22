"""
Execution slippage under normal and stress conditions.

Models half-spread + sqrt impact + volatility component, scaled up when
VPIN/informed flow, wide spreads, or high ATR indicate stress.
"""
from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass
class SlippageResult:
    slip_bps: float
    stress: bool
    fill_price: float


def estimate_slippage_bps(
    *,
    side: str,
    spread_bps: float = 3.0,
    atr_pct: float = 0.5,
    order_usd: float = 1000.0,
    adv_usd: float = 1_000_000.0,
    vpin: float = 0.0,
    volume_ratio: float = 1.0,
    stress_scale: float = 1.0,
) -> tuple[float, bool]:
    """
    Returns (slippage_bps one-way, stress_flag).

    stress when VPIN high, spread wide, or volume thin relative to size.
    stress_scale: online calibrator multiplier (default 1.0).
    """
    half = max(spread_bps, 0.0) / 2.0
    participation = order_usd / max(adv_usd, order_usd, 1.0)
    # Almgren-style sqrt participation → bps
    impact_bps = 8.0 * math.sqrt(max(participation, 1e-8)) * 100.0
    vol_bps = max(atr_pct, 0.0) * 5.0  # ATR% → bps contribution

    stress = (
        vpin >= 0.7
        or spread_bps >= 25.0
        or volume_ratio < 0.5
        or participation > 0.02
    )
    mult = 1.0
    if stress:
        mult = 1.5 + min(max(vpin, 0.0), 1.0) * 0.5
        if participation > 0.05:
            mult += 0.5

    slip = (half + impact_bps + vol_bps) * mult * max(stress_scale, 0.5)
    return float(min(max(slip, 0.0), 200.0)), stress


def apply_slippage(price: float, side: str, slip_bps: float) -> float:
    """BUY pays up; SELL receives down."""
    if price <= 0:
        return price
    adj = price * (slip_bps / 10_000.0)
    if side.upper() == "BUY":
        return price + adj
    return price - adj


def slipped_fill(
    price: float,
    side: str,
    *,
    spread_bps: float = 3.0,
    atr_pct: float = 0.5,
    order_usd: float = 1000.0,
    adv_usd: float = 1_000_000.0,
    vpin: float = 0.0,
    volume_ratio: float = 1.0,
    stress_scale: float = 1.0,
) -> SlippageResult:
    slip, stress = estimate_slippage_bps(
        side=side,
        spread_bps=spread_bps,
        atr_pct=atr_pct,
        order_usd=order_usd,
        adv_usd=adv_usd,
        vpin=vpin,
        volume_ratio=volume_ratio,
        stress_scale=stress_scale,
    )
    return SlippageResult(
        slip_bps=slip,
        stress=stress,
        fill_price=apply_slippage(price, side, slip),
    )
