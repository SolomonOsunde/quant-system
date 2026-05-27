"""
Momentum Factors
================
Implements:
  - Time-Series Momentum — Moskowitz, Ooi, Pedersen (2012)
    Positions proportional to the sign of past returns, scaled by volatility.
  - Cross-Sectional Momentum — Jegadeesh & Titman (1993)
    Long top-ranked, short bottom-ranked assets by trailing return.
  - Idiosyncratic Momentum — Blitz, Huij, Martens (2011)
    Momentum orthogonal to BTC (market-beta adjusted).
  - Momentum quality: consistency across sub-periods.

All scores are compressed to [-1, 1] via tanh or rank normalization.
Skip-period (last 60 ticks) avoids contamination by bid-ask bounce reversal.
"""

import numpy as np
from dataclasses import dataclass
from typing import Optional
from loguru import logger


@dataclass
class MomentumResult:
    ts_momentum:       float   # time-series, vol-scaled [-1, 1]
    vol_adj_momentum:  float   # Sharpe-ratio momentum [-1, 1]
    cs_rank:           float   # cross-sectional rank [-1, 1]; +1 = top performer
    idiosyncratic:     float   # beta-neutral momentum [-1, 1]
    quality:           float   # fraction of sub-periods confirming direction [0, 1]
    direction:         int     # +1, -1, 0
    confidence:        float   # 0-1


class MomentumEngine:
    """
    Stateless momentum calculator. All inputs are provided per call.
    Call compute_ts_momentum() per symbol, compute_cross_sectional()
    and compute_idiosyncratic() once per evaluation cycle for all symbols.
    """

    # Lookback windows (in ticks). At ~2 ticks/s: 600=5min, 3600=30min, 14400=2h.
    LOOKBACKS = {
        "5m":  600,
        "30m": 3600,
        "2h":  14400,
        "8h":  57600,
    }
    SKIP_TICKS = 60           # skip most recent N ticks (bid-ask bounce avoidance)
    MIN_TICKS  = 200          # minimum for any computation

    def compute_ts_momentum(self, prices: list[float]) -> Optional[MomentumResult]:
        """
        Multi-lookback time-series momentum for a single symbol.
        Each lookback is vol-scaled (Sharpe-like) before averaging.
        """
        if len(prices) < self.MIN_TICKS:
            return None

        arr  = np.array([p for p in prices if p > 0], dtype=float)
        rets = np.diff(arr) / arr[:-1]

        ts_scores   = []
        sub_quality = []

        for label, window in self.LOOKBACKS.items():
            if len(arr) < window + self.SKIP_TICKS:
                continue

            end   = len(rets) - self.SKIP_TICKS
            start = max(0, end - window)
            formation = rets[start:end]

            if len(formation) < 20:
                continue

            period_ret = float(np.sum(formation))
            realized_vol = float(np.std(formation)) * np.sqrt(max(len(formation), 1))

            if realized_vol < 1e-8:
                continue

            vol_scaled = period_ret / realized_vol
            ts_scores.append(float(np.tanh(vol_scaled * 0.5)))

            # Quality: fraction of 20-tick sub-periods that agree with direction
            n_sub = max(1, len(formation) // 20)
            sub_rets = [
                np.sum(formation[i:i + n_sub])
                for i in range(0, len(formation) - n_sub, n_sub)
            ]
            if sub_rets:
                direction_sign = 1 if period_ret >= 0 else -1
                sub_quality.append(np.mean([np.sign(r) == direction_sign for r in sub_rets]))

        if not ts_scores:
            return None

        ts_mom  = float(np.mean(ts_scores))
        quality = float(np.mean(sub_quality)) if sub_quality else 0.5

        # Short-term vol-adjusted momentum (last 5m, no skip)
        short_rets = rets[-min(600, len(rets)):]
        sr_ret = float(np.sum(short_rets))
        sr_vol = float(np.std(short_rets) * np.sqrt(len(short_rets))) if len(short_rets) > 1 else 1e-8
        vol_adj = float(np.tanh(sr_ret / max(sr_vol, 1e-8) * 0.5))

        direction  = 1 if ts_mom > 0.05 else (-1 if ts_mom < -0.05 else 0)
        confidence = min(1.0, abs(ts_mom) * quality * 1.5)

        return MomentumResult(
            ts_momentum      = round(ts_mom, 4),
            vol_adj_momentum = round(vol_adj, 4),
            cs_rank          = 0.0,   # filled by compute_cross_sectional
            idiosyncratic    = 0.0,   # filled by compute_idiosyncratic
            quality          = round(quality, 4),
            direction        = direction,
            confidence       = round(confidence, 4),
        )

    def compute_cross_sectional(
        self, symbol: str, all_prices: dict[str, list[float]]
    ) -> float:
        """
        Cross-sectional rank of symbol's recent return versus all peers.
        Returns rank normalized to [-1, 1]: +1 = best performer, -1 = worst.

        Uses a 30-minute formation window with a 60-tick skip period.
        """
        if symbol not in all_prices or len(all_prices) < 2:
            return 0.0

        window = 3600   # ~30 minutes of ticks

        returns: dict[str, float] = {}
        for sym, prices in all_prices.items():
            arr = np.array([p for p in prices if p > 0], dtype=float)
            if len(arr) < 100 or arr[0] <= 0:
                continue

            skip  = min(self.SKIP_TICKS, len(arr) // 10)
            start = max(0, len(arr) - window - skip)
            end   = len(arr) - skip

            if end <= start or arr[start] <= 0:
                continue

            returns[sym] = float((arr[end - 1] - arr[start]) / arr[start])

        if symbol not in returns or len(returns) < 2:
            return 0.0

        syms   = sorted(returns.keys())
        values = np.array([returns[s] for s in syms])
        idx    = syms.index(symbol)
        rank   = int(np.argsort(np.argsort(values))[idx])   # 0 = lowest

        normalized = (rank / (len(syms) - 1)) * 2.0 - 1.0
        return round(float(normalized), 4)

    def compute_idiosyncratic(
        self,
        symbol: str,
        all_prices: dict[str, list[float]],
        btc_symbol: str = "BTC/USD",
    ) -> float:
        """
        Idiosyncratic momentum: return orthogonal to BTC's movement.
        Removes the common crypto factor via OLS beta, leaving asset-specific alpha.

        Blitz, Huij, Martens (2011): idiosyncratic momentum has higher
        information ratio than raw momentum.
        """
        if symbol == btc_symbol:
            return 0.0
        if symbol not in all_prices or btc_symbol not in all_prices:
            return 0.0

        sym_prices = np.array([p for p in all_prices[symbol] if p > 0], dtype=float)
        btc_prices = np.array([p for p in all_prices[btc_symbol] if p > 0], dtype=float)

        n = min(len(sym_prices), len(btc_prices), 7200)   # up to 1h of ticks
        if n < 100:
            return 0.0

        sym_ret = np.diff(sym_prices[-n:]) / sym_prices[-n:-1]
        btc_ret = np.diff(btc_prices[-n:]) / btc_prices[-n:-1]

        # OLS beta via closed-form
        cov_mat  = np.cov(sym_ret, btc_ret)
        beta     = cov_mat[0, 1] / max(cov_mat[1, 1], 1e-12)

        idio_rets    = sym_ret - beta * btc_ret
        total_return = float(np.sum(idio_rets))
        vol          = float(np.std(idio_rets) * np.sqrt(len(idio_rets)))

        return float(np.tanh(total_return / max(vol, 1e-8) * 0.5))
