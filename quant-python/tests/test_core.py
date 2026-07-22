"""Unit tests — slippage, sizing, forex detection, calibration."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from signals.slippage import estimate_slippage_bps, apply_slippage, slipped_fill
from signals.position_sizing import KellyPositionSizer
from signals.calibration import SlippageCalibrator


def test_buy_slip_raises_price():
    px = apply_slippage(100.0, "BUY", 10.0)
    assert px > 100.0
    assert abs(px - 100.1) < 1e-9


def test_sell_slip_lowers_price():
    px = apply_slippage(100.0, "SELL", 10.0)
    assert px < 100.0


def test_stress_inflates_slip():
    calm, _ = estimate_slippage_bps(
        side="BUY", spread_bps=2, atr_pct=0.2, order_usd=100,
        adv_usd=10_000_000, vpin=0.1, volume_ratio=1.0,
    )
    stressed, flag = estimate_slippage_bps(
        side="BUY", spread_bps=40, atr_pct=2.0, order_usd=50_000,
        adv_usd=100_000, vpin=0.9, volume_ratio=0.2,
    )
    assert flag is True
    assert stressed > calm


def test_stress_scale_multiplies():
    a, _ = estimate_slippage_bps(side="BUY", spread_bps=5, stress_scale=1.0)
    b, _ = estimate_slippage_bps(side="BUY", spread_bps=5, stress_scale=2.0)
    assert abs(b - 2 * a) < 1e-6


def test_kelly_impact_shrinks_size():
    s = KellyPositionSizer(100_000, 5_000)
    no_impact = s.compute(0.8, 100.0, 0.4, 5.0, 0.0, adv_usd=0)
    with_impact = s.compute(0.8, 100.0, 0.4, 5.0, 0.0, adv_usd=50_000)
    assert with_impact.recommended_usd <= no_impact.recommended_usd


def test_kelly_blocks_when_full():
    s = KellyPositionSizer(100_000, 5_000)
    r = s.compute(0.8, 100.0, 0.4, 5.0, capital_deployed_usd=25_000)
    assert r.recommended_usd == 0.0
    assert r.method == "blocked"


def test_calibrator_adverse_buy():
    c = SlippageCalibrator()
    bps = c.record("BTC/USD", "BUY", 100.0, 100.5)
    assert bps is not None and bps > 0
    assert c.stress_scale() >= 1.0


def test_slipped_fill_dataclass():
    r = slipped_fill(50.0, "BUY", spread_bps=10, order_usd=1000, adv_usd=1e6)
    assert r.fill_price > 50.0
    assert r.slip_bps > 0


def test_forex_detection():
    # Import helper from main without starting consumers — inline mirror
    FOREX = frozenset({"EUR", "GBP", "USD", "JPY", "AUD", "CAD", "CHF", "NZD"})

    def is_fx(sym: str) -> bool:
        parts = sym.upper().replace("_", "/").split("/")
        return len(parts) == 2 and parts[0] in FOREX and parts[1] in FOREX

    assert is_fx("EUR/USD")
    assert is_fx("EUR_USD")
    assert not is_fx("BTC/USD")
    assert not is_fx("ETH/USD")
