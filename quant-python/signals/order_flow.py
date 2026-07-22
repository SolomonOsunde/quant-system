"""
Order Flow Analysis
===================
Lee-Ready classification, OFI, VPIN — Cont / Easley et al.

Per-symbol tick deques so busy symbols cannot evict quiet ones from a
shared global window.
"""

import numpy as np
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Optional


@dataclass
class OrderFlowResult:
    ofi_short:       float
    ofi_medium:      float
    ofi_long:        float
    ofi_slope:       float
    vpin:            float
    trade_pressure:  float
    cum_ofi:         float
    informed_flag:   bool
    direction:       int
    confidence:      float


class OrderFlowAnalyzer:
    VPIN_WINDOW_BUCKETS = 50
    INFORMED_THRESHOLD  = 0.70
    CUM_OFI_DECAY       = 0.999

    def __init__(self, window: int = 5000):
        self._window = window
        # Per-symbol rolling windows — no cross-eviction
        self._ticks: dict[str, deque] = defaultdict(lambda: deque(maxlen=window))

        self._prev_mid:   dict[str, float] = {}
        self._prev_price: dict[str, float] = {}
        self._cum_ofi:    dict[str, float] = {}

        self._bucket_buy:  dict[str, float] = {}
        self._bucket_sell: dict[str, float] = {}
        self._bucket_vol:  dict[str, float] = {}
        self._bucket_size: dict[str, float] = {}
        self._vpin_buckets: dict[str, deque] = {}

    def update(self, tick: dict):
        symbol = tick.get("symbol", "")
        price  = tick.get("last") or tick.get("bid", 0)
        bid    = tick.get("bid", price)
        ask    = tick.get("ask", price)
        volume = float(tick.get("volume", 0) or 0)

        if not symbol or price <= 0 or volume <= 0:
            return

        mid = (bid + ask) / 2.0

        prev_mid   = self._prev_mid.get(symbol, mid)
        prev_price = self._prev_price.get(symbol, price)

        if price > prev_mid + 1e-10:
            side = 1
        elif price < prev_mid - 1e-10:
            side = -1
        else:
            side = 1 if price >= prev_price else -1

        self._prev_mid[symbol]   = mid
        self._prev_price[symbol] = price

        classified = {**tick, "_side": side, "_mid": mid}
        self._ticks[symbol].append(classified)

        prev_cum = self._cum_ofi.get(symbol, 0.0)
        self._cum_ofi[symbol] = prev_cum * self.CUM_OFI_DECAY + side * volume

        self._update_vpin(symbol, volume, side)

    def _update_vpin(self, symbol: str, volume: float, side: int):
        if symbol not in self._vpin_buckets:
            self._vpin_buckets[symbol]  = deque(maxlen=self.VPIN_WINDOW_BUCKETS)
            self._bucket_buy[symbol]    = 0.0
            self._bucket_sell[symbol]   = 0.0
            self._bucket_vol[symbol]    = 0.0
            self._bucket_size[symbol]   = None

        if self._bucket_size[symbol] is None:
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
        ticks = list(self._ticks.get(symbol, ()))
        if len(ticks) < 50:
            return None

        ofi_short  = self._ofi(ticks, 100)
        ofi_medium = self._ofi(ticks, 500)
        ofi_long   = self._ofi(ticks, 2000)
        ofi_slope  = ofi_short - ofi_medium

        pressure = self._trade_pressure(ticks, 300)

        buckets = list(self._vpin_buckets.get(symbol, []))
        vpin = float(np.mean(buckets)) if buckets else 0.3

        cum = self._cum_ofi.get(symbol, 0.0)
        recent_vols = [t.get("volume", 0) for t in ticks[-500:]]
        total_vol   = max(sum(recent_vols), 1e-8)
        cum_norm    = float(np.tanh(cum / total_vol))

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
        recent   = ticks[-window:] if len(ticks) > window else ticks
        buy_vol  = sum(t.get("volume", 0) for t in recent if t.get("_side") == 1)
        sell_vol = sum(t.get("volume", 0) for t in recent if t.get("_side") == -1)
        total    = buy_vol + sell_vol
        return (buy_vol - sell_vol) / total if total > 1e-10 else 0.0

    def _trade_pressure(self, ticks: list, window: int) -> float:
        recent   = ticks[-window:] if len(ticks) > window else ticks
        buy_cnt  = sum(1 for t in recent if t.get("_side") == 1)
        sell_cnt = sum(1 for t in recent if t.get("_side") == -1)
        total    = buy_cnt + sell_cnt
        return (buy_cnt - sell_cnt) / total if total > 0 else 0.0
