"""
Position Sizing — Kelly Criterion + Volatility Targeting
=========================================================
Implements:
  - Full Kelly with Bayesian shrinkage toward 50% win rate prior
  - Half-Kelly as the practical operating point (Thorp 1997, 2006)
  - Volatility targeting: size so each position contributes a fixed
    fraction of daily portfolio variance — Hurst, Ooi, Pedersen (2013)
  - Transaction cost penalty: reduce size when spread costs erode edge
  - Concentration cap: no single position exceeds MAX_POSITION_PCT

Kelly is the mathematically optimal long-run bet size given edge and variance.
In practice, half-Kelly is used because (a) parameter estimation error causes
systematic overbetting, and (b) the Kelly criterion is asymmetrically harsh —
overbetting by 2× destroys wealth at the same rate as full Kelly builds it.

Bayesian shrinkage toward a 50% win rate prevents overconfidence when the
signal history is short (< 30 samples). With N trades the posterior win rate
is: p* = (N·p_empirical + M·0.5) / (N + M) where M = shrinkage_prior_n.
"""

import numpy as np
from collections import deque
from dataclasses import dataclass
from loguru import logger


@dataclass
class SizingResult:
    kelly_fraction:   float   # full Kelly f* (for reference)
    half_kelly:       float   # operating Kelly fraction
    vol_target_units: float   # units from volatility targeting
    recommended_usd:  float   # final recommended USD position size
    method:           str     # "kelly" | "vol_target" | "minimum"


class KellyPositionSizer:
    """
    Signal-history-aware Kelly sizer. Record trade outcomes via
    record_outcome() after each fill; compute() returns sizing
    for the next trade.
    """

    TARGET_DAILY_VOL  = 0.015    # 1.5% daily portfolio volatility target
    MAX_POSITION_PCT  = 0.20     # hard cap: no more than 20% of capital
    MIN_POSITION_PCT  = 0.005    # floor: always bet at least 0.5%
    KELLY_MULTIPLIER  = 0.50     # half-Kelly
    SHRINKAGE_PRIOR   = 0.50     # prior win rate (fair coin)
    SHRINKAGE_N       = 25       # equivalent prior observations
    MAX_HISTORY       = 300      # rolling trade history length

    def __init__(self, capital_usd: float, max_order_usd: float):
        self.capital       = max(capital_usd, 1.0)
        self.max_order_usd = max_order_usd
        self._history: deque[dict] = deque(maxlen=self.MAX_HISTORY)

    def record_outcome(self, pnl_usd: float, signal_confidence: float):
        """Record a completed trade for Kelly parameter estimation."""
        self._history.append({"pnl": pnl_usd, "conf": signal_confidence, "win": pnl_usd > 0})

    def compute(
        self,
        confidence:    float,
        price:         float,
        realized_vol:  float,   # annualized realized vol (decimal, e.g. 0.8 = 80%)
        spread_bps:    float,
        capital_deployed_usd: float = 0.0,
    ) -> SizingResult:
        """
        Compute recommended USD position size.

        Parameters
        ----------
        confidence          : model confidence for this signal [0, 1]
        price               : current asset price
        realized_vol        : annualized realized volatility (decimal)
        spread_bps          : current bid-ask spread in basis points
        capital_deployed_usd: existing exposure in same symbol (for concentration limit)
        """

        # 1. Kelly fraction from signal history
        kelly_f = self._estimate_kelly(confidence)

        # 2. Volatility-target units
        vol_units = self._vol_target_units(realized_vol, price)

        # 3. Transaction cost penalty (spread erodes edge)
        tc_penalty = self._tc_penalty(spread_bps)

        # 4. Concentration: remaining room before hitting per-symbol cap
        room = max(0.0, self.capital * self.MAX_POSITION_PCT - capital_deployed_usd)

        # 5. Final size: conservative of Kelly and vol-target, then apply penalties
        kelly_usd  = min(self.capital * kelly_f, self.max_order_usd)
        vol_usd    = vol_units * price if price > 0 else self.max_order_usd

        raw_usd    = min(kelly_usd, vol_usd, room)
        adj_usd    = raw_usd * tc_penalty
        final_usd  = float(np.clip(
            adj_usd,
            self.capital * self.MIN_POSITION_PCT,
            self.max_order_usd,
        ))

        method = "kelly" if kelly_usd <= vol_usd else "vol_target"
        if adj_usd <= self.capital * self.MIN_POSITION_PCT:
            method = "minimum"

        return SizingResult(
            kelly_fraction   = round(kelly_f, 5),
            half_kelly       = round(kelly_f * self.KELLY_MULTIPLIER, 5),
            vol_target_units = round(vol_units, 6),
            recommended_usd  = round(final_usd, 2),
            method           = method,
        )

    # ── Internal helpers ────────────────────────────────────────────────

    def _estimate_kelly(self, confidence: float) -> float:
        """
        Estimate Kelly fraction with Bayesian shrinkage.

        With sufficient history we use empirical win rate and win/loss ratio.
        With thin history we shrink toward confidence-derived prior.
        """
        n = len(self._history)

        if n < 10:
            # Prior-only: use confidence as a proxy for edge
            raw_f = max(0.0, 2.0 * confidence - 1.0)
            return float(np.clip(raw_f * self.KELLY_MULTIPLIER, 0.0, self.MAX_POSITION_PCT))

        wins   = [t for t in self._history if t["win"]]
        losses = [t for t in self._history if not t["win"]]

        # Bayesian posterior win rate
        p_emp = len(wins) / n
        p_post = (
            (p_emp * n + self.SHRINKAGE_PRIOR * self.SHRINKAGE_N)
            / (n + self.SHRINKAGE_N)
        )

        # Win/loss payoff ratio
        if wins and losses:
            b = (np.mean([abs(t["pnl"]) for t in wins])
                 / max(np.mean([abs(t["pnl"]) for t in losses]), 1e-8))
        elif wins:
            b = 2.0    # optimistic prior when all wins
        else:
            b = 0.5    # pessimistic prior when all losses

        # Kelly formula: f* = (p·b − q) / b
        q     = 1.0 - p_post
        raw_f = (p_post * b - q) / max(b, 1e-8)
        half_f = raw_f * self.KELLY_MULTIPLIER

        return float(np.clip(half_f, 0.0, self.MAX_POSITION_PCT))

    def _vol_target_units(self, realized_vol: float, price: float) -> float:
        """
        Units such that position contributes TARGET_DAILY_VOL to portfolio.

        units = (target_daily_vol × capital) / (daily_vol_per_unit)
              = (target_daily_vol × capital) / (annual_vol / sqrt(365) × price)
        """
        if realized_vol < 1e-6 or price <= 0:
            return 0.0

        daily_vol_per_unit = (realized_vol / np.sqrt(365)) * price
        target_usd         = self.TARGET_DAILY_VOL * self.capital
        return float(target_usd / max(daily_vol_per_unit, 1e-8))

    def _tc_penalty(self, spread_bps: float) -> float:
        """
        Penalize position size when round-trip transaction costs are high
        relative to expected edge. At 150 bps the multiplier is 0.25.
        """
        penalty = max(0.15, 1.0 - spread_bps / 200.0)
        return float(penalty)
