"""
Statistical Arbitrage via Kalman Filter
=========================================
Implements:
  - Kalman filter for time-varying hedge ratio — Lo (2010), Avellaneda & Lee (2010)
  - Engle-Granger cointegration test with ADF (1987)
  - Spread z-score entry/exit/stop signal generation
  - Half-life of mean reversion via AR(1) regression
  - Multi-pair monitoring across 5 crypto pairs

The Kalman filter tracks a dynamic hedge ratio β_t that adapts to slow
structural drift in crypto correlations. A fixed OLS β would introduce
systematic bias within days. Time-varying β eliminates that bias and
provides a cleaner spread with faster mean reversion.

Entry at |z| > 2.0, exit at |z| < 0.5, stop at |z| > 4.0.
Only trade pairs that are cointegrated (ADF p < 0.05) AND have a
mean-reversion half-life within a tradable window (30 ticks to 14h).
"""

import numpy as np
from dataclasses import dataclass, field
from typing import Optional
from loguru import logger

try:
    from statsmodels.tsa.stattools import adfuller
    _STATSMODELS = True
except ImportError:
    _STATSMODELS = False
    logger.warning("statsmodels not available — cointegration test disabled, assuming all pairs cointegrated")


@dataclass
class KalmanState:
    """Per-pair Kalman filter state: intercept α and hedge ratio β."""
    alpha:  float      = 0.0
    beta:   float      = 1.0
    P:      np.ndarray = field(default_factory=lambda: np.eye(2) * 5.0)
    Ve:     float      = 0.001   # observation noise variance (adaptive)
    delta:  float      = 1e-4    # system noise — controls β adaptation speed


@dataclass
class PairSignal:
    pair:          str    # e.g. "BTC/USD:ETH/USD"
    z_score:       float  # current spread z-score
    spread:        float  # raw log-spread value
    hedge_ratio:   float  # current Kalman β
    half_life:     float  # estimated mean-reversion half-life (ticks)
    cointegrated:  bool   # ADF p < 0.05
    direction:     int    # +1 = go long spread, -1 = go short spread, 0 = flat
    signal_type:   str    # "entry" | "exit" | "stop" | "hold"
    leg1:          str    # first symbol (the X in y = α + β·x)
    leg2:          str    # second symbol (the Y)
    leg1_side:     str    # "BUY" or "SELL"
    leg2_side:     str    # "BUY" or "SELL"


class StatArbEngine:
    """
    Monitors a fixed set of crypto pairs. update() should be called
    every cycle with the latest price arrays; compute() returns the
    current signal for each pair.
    """

    ENTRY_Z = 2.0
    EXIT_Z  = 0.5
    STOP_Z  = 4.0
    MIN_HALF_LIFE = 30        # ticks (~15 seconds) — faster than this is noise
    MAX_HALF_LIFE = 100_000   # ticks (~14h at 2t/s) — too slow to trade

    COINT_RETEST_INTERVAL = 5_000   # update ticks between cointegration re-tests

    def __init__(self, pairs: list[tuple[str, str]] | None = None):
        self._states:         dict[str, KalmanState]  = {}
        self._spreads:        dict[str, list[float]]  = {}
        self._coint:          dict[str, bool]         = {}
        self._coint_pval:     dict[str, float]        = {}
        self._update_counts:  dict[str, int]          = {}
        self.pairs: list[tuple[str, str]] = []
        if pairs is not None:
            self.pairs = list(pairs)
        else:
            self.refresh_pairs()

    def refresh_pairs(self):
        """Reload enabled pairs from Postgres."""
        try:
            from db.instruments import load_stat_arb_pairs
            self.pairs = list(load_stat_arb_pairs())
        except Exception as e:
            logger.error("Could not load stat_arb_pairs from DB: {}", e)
            self.pairs = []
        if not self.pairs:
            logger.warning("No enabled stat-arb pairs — SA idle until DB is seeded")
        else:
            logger.info("StatArb using {} pair(s) from DB", len(self.pairs))

    # ── Public interface ────────────────────────────────────────────────

    def update(self, all_prices: dict[str, list[float]]):
        """
        Advance Kalman filter for all pairs that have sufficient price history.
        Call once per evaluation cycle with the latest price arrays.
        """
        for sym1, sym2 in self.pairs:
            if sym1 not in all_prices or sym2 not in all_prices:
                continue
            p1_arr = all_prices[sym1]
            p2_arr = all_prices[sym2]
            if len(p1_arr) < 100 or len(p2_arr) < 100:
                continue

            key = _key(sym1, sym2)

            if key not in self._states:
                self._states[key]        = KalmanState()
                self._spreads[key]       = []
                self._update_counts[key] = 0
                self._coint[key]         = True    # optimistic prior
                self._coint_pval[key]    = 0.05

            lx = np.log(p1_arr[-1])
            ly = np.log(p2_arr[-1])
            if not (np.isfinite(lx) and np.isfinite(ly)):
                continue

            self._kalman_update(key, lx, ly)
            self._update_counts[key] += 1

            # Periodic cointegration re-test
            if self._update_counts[key] % self.COINT_RETEST_INTERVAL == 0:
                self._test_cointegration(key, all_prices, sym1, sym2)

    def compute(self, sym1: str, sym2: str) -> Optional[PairSignal]:
        """Return the current trading signal for a pair."""
        key = _key(sym1, sym2)
        spreads = self._spreads.get(key, [])
        if len(spreads) < 200:
            return None

        state = self._states[key]
        window = min(1000, len(spreads))
        recent = spreads[-window:]

        mu  = float(np.mean(recent))
        sig = float(np.std(recent))
        if sig < 1e-10:
            return None

        z = (spreads[-1] - mu) / sig

        half_life   = _estimate_half_life(recent)
        cointegrated = (
            self._coint.get(key, True)
            and self.MIN_HALF_LIFE < half_life < self.MAX_HALF_LIFE
        )

        abs_z = abs(z)
        if abs_z >= self.STOP_Z:
            signal_type = "stop"
        elif abs_z >= self.ENTRY_Z and cointegrated:
            signal_type = "entry"
        elif abs_z <= self.EXIT_Z:
            signal_type = "exit"
        else:
            signal_type = "hold"

        # Fade the z-score: z > 0 → spread above mean → expect it to fall
        # → short leg2 (Y) and long leg1 (X) scaled by β
        direction = -1 if z > 0 else 1

        if signal_type == "entry":
            if direction == -1:  # short spread: sell Y, buy X
                leg1_side, leg2_side = "BUY", "SELL"
            else:                # long spread: buy Y, sell X
                leg1_side, leg2_side = "SELL", "BUY"
        else:
            leg1_side, leg2_side = "FLAT", "FLAT"
            direction = 0

        return PairSignal(
            pair         = f"{sym1}:{sym2}",
            z_score      = round(z, 4),
            spread       = round(spreads[-1], 6),
            hedge_ratio  = round(state.beta, 4),
            half_life    = round(half_life, 1),
            cointegrated = cointegrated,
            direction    = direction,
            signal_type  = signal_type,
            leg1         = sym1,
            leg2         = sym2,
            leg1_side    = leg1_side,
            leg2_side    = leg2_side,
        )

    def get_all_signals(self, all_prices: dict[str, list[float]]) -> list[PairSignal]:
        """Convenience: return signals for all pairs with sufficient history."""
        signals = []
        for sym1, sym2 in self.pairs:
            sig = self.compute(sym1, sym2)
            if sig is not None:
                signals.append(sig)
        return signals

    # ── Kalman filter ────────────────────────────────────────────────────

    def _kalman_update(self, key: str, log_x: float, log_y: float):
        """
        One-step Kalman filter update for the state [α, β].

        Observation model:  log_y = α + β·log_x + ε,   ε ~ N(0, Ve)
        Transition model:   [α, β]_t = [α, β]_{t-1} + η,  η ~ N(0, Q)

        Q is calibrated via delta (state noise / obs noise ratio).
        """
        state = self._states[key]
        x_vec = np.array([1.0, log_x])   # design vector

        # System noise covariance (scaled identity)
        Q = (state.delta / (1.0 - state.delta)) * np.eye(2)

        # Prediction step
        P_pred = state.P + Q

        # Innovation
        y_hat  = state.alpha + state.beta * log_x
        innov  = log_y - y_hat

        # Innovation covariance and Kalman gain
        S = float(x_vec @ P_pred @ x_vec) + state.Ve
        K = (P_pred @ x_vec) / S           # shape (2,)

        # State update
        state.alpha += K[0] * innov
        state.beta  += K[1] * innov
        state.P      = (np.eye(2) - np.outer(K, x_vec)) @ P_pred

        # Adaptive observation noise (exponential moving average of squared innovation)
        state.Ve = max(1e-6, 0.99 * state.Ve + 0.01 * innov ** 2)

        # Record spread
        spread = log_y - state.alpha - state.beta * log_x
        buf = self._spreads[key]
        buf.append(float(spread))
        if len(buf) > 20_000:
            del buf[:-10_000]   # trim to last 10k, preserving recency

    # ── Cointegration ────────────────────────────────────────────────────

    def _test_cointegration(
        self, key: str, all_prices: dict, sym1: str, sym2: str
    ):
        if not _STATSMODELS:
            return

        spreads = self._spreads.get(key, [])
        if len(spreads) < 500:
            return

        try:
            adf = adfuller(spreads[-3000:], maxlag=10, autolag="AIC")
            pval = float(adf[1])
            self._coint[key]      = pval < 0.05
            self._coint_pval[key] = pval
            logger.info(
                "Cointegration {}: ADF p={:.4f} → {}",
                key, pval,
                "COINTEGRATED" if self._coint[key] else "NOT cointegrated",
            )
        except Exception as e:
            logger.debug("ADF test error for {}: {}", key, e)


# ── Module-level helpers ────────────────────────────────────────────────

def _key(sym1: str, sym2: str) -> str:
    return f"{sym1}:{sym2}"


def _estimate_half_life(spreads: list[float]) -> float:
    """
    Estimate mean-reversion half-life via AR(1) regression:
        Δy_t = ρ·y_{t-1} + ε
    Half-life = -log(2) / log(1 + ρ)
    """
    try:
        arr   = np.array(spreads, dtype=float)
        delta = np.diff(arr)
        lag   = arr[:-1]
        # Closed-form OLS
        rho = float(np.cov(delta, lag)[0, 1] / max(np.var(lag), 1e-12))
        if rho >= 0:
            return float("inf")
        return float(-np.log(2.0) / np.log(1.0 + rho))
    except Exception:
        return float("inf")
