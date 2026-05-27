"""
Order Flow Analysis
===================
Implements:
  - Lee-Ready (1991) tick classification — buyer vs seller initiated
  - Order Flow Imbalance (OFI) — Cont, Kukanov, Stoikov (2014)
  - Volume-synchronized PIN (VPIN) — Easley, López de Prado, O'Hara (2012)
  - Cumulative trade pressure normalized to [-1, +1]

Microstructure signals derived from the raw tick stream. They measure the
directional intent of informed traders, which systematically leads price
discovery. OFI is one of the strongest short-horizon predictors of returns
with an R² of ~60% at 1-minute horizons (Cont et al. 2014).
"""

import numpy as np
from collections import deque
from dataclasses import dataclass
from typing import Optional
from loguru import logger


@dataclass
class OrderFlowResult:
    ofi_short:       float   # OFI over last 100 ticks, normalized [-1, 1]
    ofi_medium:      float   # OFI over last 500 ticks, normalized [-1, 1]
    ofi_long:        float   # OFI over last 2000 ticks, normalized [-1, 1]
    ofi_slope:       float   # rate of change of OFI (acceleration)
    vpin:            float   # 0-1: probability of informed trading
    trade_pressure:  float   # buyer vs seller count imbalance [-1, 1]
    cum_ofi:         float   # cumulative signed volume (normalized)
    informed_flag:   bool    # True if VPIN > 0.7 — high adverse selection risk
    direction:       int     # +1 buy pressure dominant, -1 sell, 0 neutral
    confidence:      float   # 0-1


class OrderFlowAnalyzer:
    """
    Maintains a rolling window of Lee-Ready classified ticks and computes
    order flow metrics in O(1) per tick via incremental updates.

    Call update() on every incoming tick, then compute() per symbol
    to get the current order flow signal.
    """

    VPIN_WINDOW_BUCKETS = 50    # rolling VPIN bucket count (Easley et al. standard)
    INFORMED_THRESHOLD  = 0.70  # VPIN above this → high adverse selection risk
    CUM_OFI_DECAY       = 0.999 # exponential decay for cumulative OFI

    def __init__(self, window: int = 5000):
        self._window = window
        self._ticks: deque = deque(maxlen=window)

        # Per-symbol state
        self._prev_mid:   dict[str, float] = {}
        self._prev_price: dict[str, float] = {}
        self._cum_ofi:    dict[str, float] = {}

        # VPIN bucket state per symbol
        self._bucket_buy:  dict[str, float] = {}
        self._bucket_sell: dict[str, float] = {}
        self._bucket_vol:  dict[str, float] = {}
        self._bucket_size: dict[str, float] = {}
        self._vpin_buckets: dict[str, deque] = {}

    def update(self, tick: dict):
        """Classify a new tick and update rolling state. O(1) per call."""
        symbol = tick.get("symbol", "")
        price  = tick.get("last") or tick.get("bid", 0)
        bid    = tick.get("bid", price)
        ask    = tick.get("ask", price)
        volume = float(tick.get("volume", 0) or 0)

        if not symbol or price <= 0 or volume <= 0:
            return

        mid = (bid + ask) / 2.0

        # ── Lee-Ready classification ─────────────────────────────────────
        prev_mid   = self._prev_mid.get(symbol, mid)
        prev_price = self._prev_price.get(symbol, price)

        if price > prev_mid + 1e-10:
            side = 1     # buyer-initiated: traded above midpoint
        elif price < prev_mid - 1e-10:
            side = -1    # seller-initiated: traded below midpoint
        else:
            # Tick test fallback (Lee-Ready rule 2)
            side = 1 if price >= prev_price else -1

        self._prev_mid[symbol]   = mid
        self._prev_price[symbol] = price

        classified = {**tick, "_side": side, "_mid": mid}
        self._ticks.append(classified)

        # ── Cumulative OFI (exponentially decayed) ───────────────────────
        prev_cum = self._cum_ofi.get(symbol, 0.0)
        self._cum_ofi[symbol] = prev_cum * self.CUM_OFI_DECAY + side * volume

        # ── VPIN bucket accumulation ─────────────────────────────────────
        self._update_vpin(symbol, volume, side)

    def _update_vpin(self, symbol: str, volume: float, side: int):
        if symbol not in self._vpin_buckets:
            self._vpin_buckets[symbol]  = deque(maxlen=self.VPIN_WINDOW_BUCKETS)
            self._bucket_buy[symbol]    = 0.0
            self._bucket_sell[symbol]   = 0.0
            self._bucket_vol[symbol]    = 0.0
            self._bucket_size[symbol]   = None  # set on first significant tick

        if self._bucket_size[symbol] is None:
            # Calibrate bucket size heuristically on first observation
            self._bucket_size[symbol] = max(50.0, volume * 200)

        self._bucket_buy[symbol]  += volume if side == 1  else 0.0
        self._bucket_sell[symbol] += volume if side == -1 else 0.0
        self._bucket_vol[symbol]  += volume

        if self._bucket_vol[symbol] >= self._bucket_size[symbol]:
            bv = self._bucket_vol[symbol]
            imbalance = abs(self._bucket_buy[symbol] - self._bucket_sell[symbol]) / bv
            self._vpin_buckets[symbol].append(imbalance)
            self._bucket_buy[symbol]  = 0.0
            self._bucket_sell[symbol] = 0.0
            self._bucket_vol[symbol]  = 0.0

    def compute(self, symbol: str) -> Optional[OrderFlowResult]:
        """Compute all order flow metrics for a symbol from the rolling window."""
        ticks = [t for t in self._ticks if t.get("symbol") == symbol]
        if len(ticks) < 50:
            return None

        # Multi-scale OFI
        ofi_short  = self._ofi(ticks, 100)
        ofi_medium = self._ofi(ticks, 500)
        ofi_long   = self._ofi(ticks, 2000)
        ofi_slope  = ofi_short - ofi_medium   # acceleration of order flow

        # Trade count pressure (independent of volume)
        pressure = self._trade_pressure(ticks, 300)

        # VPIN
        buckets = list(self._vpin_buckets.get(symbol, []))
        vpin = float(np.mean(buckets)) if buckets else 0.3

        # Normalized cumulative OFI
        cum = self._cum_ofi.get(symbol, 0.0)
        recent_vols = [t.get("volume", 0) for t in ticks[-500:]]
        total_vol   = max(sum(recent_vols), 1e-8)
        cum_norm    = float(np.tanh(cum / total_vol))

        # Composite direction: weighted multi-scale OFI
        composite  = 0.50 * ofi_short + 0.30 * ofi_medium + 0.20 * ofi_long
        direction  = 1 if composite > 0.10 else (-1 if composite < -0.10 else 0)
        confidence = min(1.0, abs(composite) * 1.5 * (1 + abs(ofi_slope) * 0.5))

        return OrderFlowResult(
            ofi_short      = round(ofi_short, 4),
            ofi_medium     = round(ofi_medium, 4),
            ofi_long       = round(ofi_long, 4),
            ofi_slope      = round(ofi_slope, 4),
            vpin           = round(vpin, 4),
            trade_pressure = round(pressure, 4),
            cum_ofi        = round(cum_norm, 4),
            informed_flag  = vpin > self.INFORMED_THRESHOLD,
            direction      = direction,
            confidence     = round(min(1.0, confidence), 4),
        )

    def _ofi(self, ticks: list, window: int) -> float:
        """
        OFI = (buy_volume - sell_volume) / total_volume for most recent N ticks.
        Normalized to [-1, 1].
        """
        recent   = ticks[-window:] if len(ticks) > window else ticks
        buy_vol  = sum(t.get("volume", 0) for t in recent if t.get("_side") == 1)
        sell_vol = sum(t.get("volume", 0) for t in recent if t.get("_side") == -1)
        total    = buy_vol + sell_vol
        return (buy_vol - sell_vol) / total if total > 1e-10 else 0.0

    def _trade_pressure(self, ticks: list, window: int) -> float:
        """Imbalance based on count of buyer vs seller initiated trades."""
        recent   = ticks[-window:] if len(ticks) > window else ticks
        buy_cnt  = sum(1 for t in recent if t.get("_side") == 1)
        sell_cnt = sum(1 for t in recent if t.get("_side") == -1)
        total    = buy_cnt + sell_cnt
        return (buy_cnt - sell_cnt) / total if total > 0 else 0.0
