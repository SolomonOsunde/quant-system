"""
Dynamic Universe Selection
==========================
Scans every symbol currently streaming and scores it on:

  Opportunity score = realized_vol × volume_ratio / (1 + spread_bps / 50)

  - realized_vol   : annualized volatility — the raw opportunity. Zero vol = nothing to trade.
  - volume_ratio   : recent volume vs rolling average — confirms the market is active.
  - spread_bps     : transaction cost. High spread destroys edge before it can be captured.

Only symbols meeting ALL hard filters enter the scored pool:
  • MIN_VOL  (0.25 ann.)  — must have measurable price movement
  • MAX_VOL  (8.0 ann.)   — exclude coins in full panic/crash (adverse selection risk)
  • MAX_SPREAD (150 bps)  — must be executable at acceptable cost
  • MIN_TICKS (100)       — must have enough history to compute signals

The top TOP_N symbols by score form the active trading universe.
All other symbols are observed but not evaluated for trade signals.

Universe refreshes every REFRESH_INTERVAL seconds (default: 5 min).
Between refreshes the cached universe is returned instantly (O(1)).
"""

import time
import numpy as np
from dataclasses import dataclass
from typing import Optional
from loguru import logger

from features.microstructure import realized_vol as compute_rv, amihud_ratio


@dataclass
class SymbolScore:
    symbol:       str
    score:        float    # composite opportunity score
    realized_vol: float    # annualized
    spread_bps:   float
    volume_ratio: float
    amihud:       float
    tick_count:   int


class UniverseManager:
    """
    Selects the live trading universe from all symbols being streamed.

    Usage
    -----
    Call refresh() periodically (e.g. every 5 minutes) with the full
    tick dictionary. Call active_universe property to get the current set.
    """

    MIN_VOL        = 0.25     # 25 % annualized — floor: must have movement
    MAX_VOL        = 8.0      # 800 % annualized — ceiling: crisis / manipulation
    MAX_SPREAD_BPS = 150.0    # bps — liquidity gate
    MIN_TICKS      = 100      # minimum observations for any metric
    TOP_N          = 25       # maximum symbols in active universe
    REFRESH_INTERVAL = 300.0  # seconds between universe re-scores (5 min)

    def __init__(self):
        self._universe:   set[str]          = set()
        self._scores:     list[SymbolScore] = []
        self._last_refresh = 0.0

    @property
    def active_universe(self) -> set[str]:
        return self._universe

    @property
    def scores(self) -> list[SymbolScore]:
        """Full scored list, sorted best first."""
        return self._scores

    def needs_refresh(self) -> bool:
        return time.time() - self._last_refresh >= self.REFRESH_INTERVAL

    def refresh(self, all_ticks: dict[str, list[dict]]) -> set[str]:
        """
        Score every symbol in all_ticks and update the active universe.
        Returns the new active universe set.
        """
        scored: list[SymbolScore] = []

        for symbol, ticks in all_ticks.items():
            if len(ticks) < self.MIN_TICKS:
                continue

            try:
                s = self._score_symbol(symbol, ticks)
                if s is not None:
                    scored.append(s)
            except Exception as e:
                logger.debug("Universe scoring error for {}: {}", symbol, e)

        # Sort by opportunity score descending
        scored.sort(key=lambda x: x.score, reverse=True)

        # Select top N that passed all hard filters
        eligible  = [s for s in scored if s.score > 0]
        top       = eligible[:self.TOP_N]
        new_univ  = {s.symbol for s in top}

        # Log changes
        entered = new_univ - self._universe
        exited  = self._universe - new_univ
        if entered:
            logger.info("Universe +{}: {}", len(entered), sorted(entered))
        if exited:
            logger.info("Universe -{}: {}", len(exited), sorted(exited))

        self._universe    = new_univ
        self._scores      = scored   # all scores, not just top N (for diagnostics)
        self._last_refresh = time.time()

        logger.info(
            "Universe refreshed: {}/{} symbols active | top: {}",
            len(new_univ), len(all_ticks),
            [s.symbol for s in top[:8]],
        )
        return new_univ

    def _score_symbol(self, symbol: str, ticks: list[dict]) -> Optional[SymbolScore]:
        """Compute opportunity score for one symbol. Returns None if hard filters fail."""
        prices = [t.get("last") or t.get("bid", 0) for t in ticks]
        prices = [p for p in prices if p > 0]
        if len(prices) < 50:
            return None

        # Spread from most recent tick
        last = ticks[-1]
        bid  = last.get("bid", 0) or 0
        ask  = last.get("ask", 0) or 0
        mid  = (bid + ask) / 2 if bid > 0 and ask > 0 else prices[-1]
        spread_bps = (ask - bid) / mid * 10_000 if mid > 0 and bid > 0 and ask > 0 else 999.0

        # Hard filters
        if spread_bps > self.MAX_SPREAD_BPS:
            return SymbolScore(symbol, 0.0, 0.0, spread_bps, 0.0, 0.0, len(ticks))

        # Realized volatility
        rv = compute_rv(prices, window=min(3600, len(prices)))
        if rv < self.MIN_VOL or rv > self.MAX_VOL:
            return SymbolScore(symbol, 0.0, rv, spread_bps, 0.0, 0.0, len(ticks))

        # Volume ratio (recent 100 vs rolling average)
        vols = [t.get("volume", 0) or 0 for t in ticks]
        recent_vol  = np.mean(vols[-100:]) if len(vols) >= 100 else 0
        baseline    = np.mean(vols[-500:]) if len(vols) >= 500 else recent_vol
        vol_ratio   = recent_vol / max(baseline, 1e-10)

        # Amihud illiquidity (lower = more liquid = better)
        amihud = amihud_ratio(ticks, window=min(500, len(ticks)))

        # Composite score: opportunity × activity / cost
        raw_score = rv * max(vol_ratio, 0.1) / (1.0 + spread_bps / 50.0)

        # Penalise very high Amihud (illiquid market)
        if amihud > 1e-4:
            raw_score *= 0.5

        return SymbolScore(
            symbol       = symbol,
            score        = round(raw_score, 6),
            realized_vol = round(rv, 4),
            spread_bps   = round(spread_bps, 2),
            volume_ratio = round(vol_ratio, 3),
            amihud       = round(amihud, 8),
            tick_count   = len(ticks),
        )

    def log_universe_table(self):
        """Log a formatted table of all scored symbols."""
        if not self._scores:
            return
        header = f"{'Symbol':<14} {'Score':>8} {'AnnVol':>8} {'Spread':>8} {'VolRatio':>9} {'Ticks':>7}"
        rows   = [header, "-" * len(header)]
        for s in self._scores[:20]:
            marker = " ◀" if s.symbol in self._universe else ""
            rows.append(
                f"{s.symbol:<14} {s.score:>8.4f} {s.realized_vol:>7.1%} "
                f"{s.spread_bps:>7.1f}bp {s.volume_ratio:>8.2f}x {s.tick_count:>7}{marker}"
            )
        logger.info("Universe scores:\n{}", "\n".join(rows))
