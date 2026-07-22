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

    resolution="tick"  — live Kafka ticks (~2/s)
    resolution="bar"   — 1-minute OHLCV closes (backtest)
    """

    # Lookback windows in samples. Tick: ~2/s. Bar: 1 sample/minute.
    TICK_LOOKBACKS = {
        "5m":  600,
        "30m": 3600,
        "2h":  14400,
        "8h":  57600,
    }
    BAR_LOOKBACKS = {
        "5m":  5,
        "30m": 30,
        "2h":  120,
        "8h":  480,
    }
    # Kept for backwards compatibility
    LOOKBACKS = TICK_LOOKBACKS

    SKIP_TICKS = 60           # skip most recent N ticks (bid-ask bounce avoidance)
    MIN_TICKS  = 200          # minimum for tick-mode computation
    MIN_BARS   = 50           # minimum for bar-mode (need ≥30m window partially)

    def _params(self, resolution: str, skip_recent: int):
        if resolution == "bar":
            lookbacks = self.BAR_LOOKBACKS
            skip = 0 if skip_recent < 0 else skip_recent
            min_n = self.MIN_BARS
            short_n = 5          # 5 minutes of 1-min bars
            cs_window = 30
            cs_min = 20
            idio_n = 120         # 2h of 1-min bars
            idio_min = 30
            quality_chunk = 5
        else:
            lookbacks = self.TICK_LOOKBACKS
            skip = self.SKIP_TICKS if skip_recent < 0 else skip_recent
            min_n = self.MIN_TICKS
            short_n = 600
            cs_window = 3600
            cs_min = 100
            idio_n = 7200
            idio_min = 100
            quality_chunk = 20
        return lookbacks, skip, min_n, short_n, cs_window, cs_min, idio_n, idio_min, quality_chunk

    def compute_ts_momentum(
        self,
        prices: list[float],
        skip_recent: int = -1,
        resolution: str = "tick",
    ) -> Optional[MomentumResult]:
        """
        Multi-lookback time-series momentum for a single symbol.
        Each lookback is vol-scaled (Sharpe-like) before averaging.

        skip_recent: override skip. Use 0 for 1-min bar closes (no bid-ask bounce).
        resolution: "tick" (live) or "bar" (1-min backtest) — sets calendar windows.
        """
        lookbacks, skip, min_n, short_n, _, _, _, _, quality_chunk = self._params(
            resolution, skip_recent
        )

        if len(prices) < min_n:
            return None

        arr  = np.array([p for p in prices if p > 0], dtype=float)
        rets = np.diff(arr) / arr[:-1]

        ts_scores   = []
        sub_quality = []
        min_formation = 3 if resolution == "bar" else 20

        for _label, window in lookbacks.items():
            if len(arr) < window + skip:
                continue

            end   = len(rets) - skip if skip > 0 else len(rets)
            start = max(0, end - window)
            formation = rets[start:end]

            if len(formation) < min_formation:
                continue

            period_ret = float(np.sum(formation))
            realized_vol = float(np.std(formation)) * np.sqrt(max(len(formation), 1))

            if realized_vol < 1e-8:
                continue

            vol_scaled = period_ret / realized_vol
            ts_scores.append(float(np.tanh(vol_scaled * 0.5)))

            n_sub = max(1, len(formation) // quality_chunk)
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

        short_rets = rets[-min(short_n, len(rets)):]
        sr_ret = float(np.sum(short_rets))
        sr_vol = float(np.std(short_rets) * np.sqrt(len(short_rets))) if len(short_rets) > 1 else 1e-8
        vol_adj = float(np.tanh(sr_ret / max(sr_vol, 1e-8) * 0.5))

        direction  = 1 if ts_mom > 0.05 else (-1 if ts_mom < -0.05 else 0)
        confidence = min(1.0, abs(ts_mom) * quality * 1.5)

        return MomentumResult(
            ts_momentum      = round(ts_mom, 4),
            vol_adj_momentum = round(vol_adj, 4),
            cs_rank          = 0.0,
            idiosyncratic    = 0.0,
            quality          = round(quality, 4),
            direction        = direction,
            confidence       = round(confidence, 4),
        )

    def compute_cross_sectional(
        self,
        symbol: str,
        all_prices: dict[str, list[float]],
        resolution: str = "tick",
    ) -> float:
        """
        Cross-sectional rank of symbol's recent return versus all peers.
        Returns rank normalized to [-1, 1]: +1 = best performer, -1 = worst.
        """
        if symbol not in all_prices or len(all_prices) < 2:
            return 0.0

        _, skip_default, _, _, window, cs_min, _, _, _ = self._params(resolution, -1)
        skip = 0 if resolution == "bar" else skip_default

        returns: dict[str, float] = {}
        for sym, prices in all_prices.items():
            arr = np.array([p for p in prices if p > 0], dtype=float)
            if len(arr) < cs_min or arr[0] <= 0:
                continue

            skip_i = min(skip, len(arr) // 10) if skip > 0 else 0
            start = max(0, len(arr) - window - skip_i)
            end   = len(arr) - skip_i

            if end <= start or arr[start] <= 0:
                continue

            returns[sym] = float((arr[end - 1] - arr[start]) / arr[start])

        if symbol not in returns or len(returns) < 2:
            return 0.0

        syms   = sorted(returns.keys())
        values = np.array([returns[s] for s in syms])
        idx    = syms.index(symbol)
        rank   = int(np.argsort(np.argsort(values))[idx])

        normalized = (rank / (len(syms) - 1)) * 2.0 - 1.0
        return round(float(normalized), 4)

    def compute_idiosyncratic(
        self,
        symbol: str,
        all_prices: dict[str, list[float]],
        btc_symbol: str = "BTC/USD",
        resolution: str = "tick",
    ) -> float:
        """
        Idiosyncratic momentum: return orthogonal to BTC's movement.
        Removes the common crypto factor via OLS beta, leaving asset-specific alpha.
        """
        if symbol == btc_symbol:
            return 0.0
        if symbol not in all_prices or btc_symbol not in all_prices:
            return 0.0

        _, _, _, _, _, _, idio_n, idio_min, _ = self._params(resolution, -1)

        sym_prices = np.array([p for p in all_prices[symbol] if p > 0], dtype=float)
        btc_prices = np.array([p for p in all_prices[btc_symbol] if p > 0], dtype=float)

        n = min(len(sym_prices), len(btc_prices), idio_n)
        if n < idio_min:
            return 0.0

        sym_ret = np.diff(sym_prices[-n:]) / sym_prices[-n:-1]
        btc_ret = np.diff(btc_prices[-n:]) / btc_prices[-n:-1]

        cov_mat  = np.cov(sym_ret, btc_ret)
        beta     = cov_mat[0, 1] / max(cov_mat[1, 1], 1e-12)

        idio_rets    = sym_ret - beta * btc_ret
        total_return = float(np.sum(idio_rets))
        vol          = float(np.std(idio_rets) * np.sqrt(len(idio_rets)))

        return float(np.tanh(total_return / max(vol, 1e-8) * 0.5))
