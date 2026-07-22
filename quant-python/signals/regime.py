"""
Market Regime Detection
=======================
Implements:
  - Realized volatility regime (low / normal / high / crisis)
  - Hurst exponent via R/S analysis — Mandelbrot & Wallis (1969)
    H < 0.45 → mean-reverting, H > 0.55 → trending, H ≈ 0.5 → random walk
  - 3-state Gaussian HMM (bull / bear / sideways) — Hamilton (1989)
  - Regime-conditional strategy weights: which alpha sources dominate each regime

Regime-aware portfolio construction is the primary difference between
systematic managers that survive volatility crises and those that don't.
The same momentum strategy that earns +40% annualized in trending regimes
loses -20% in mean-reverting regimes. Gate it properly.
"""

import math
import numpy as np
from dataclasses import dataclass
from enum import IntEnum
from typing import Optional
from loguru import logger

try:
    from hmmlearn.hmm import GaussianHMM
    _HMM_AVAILABLE = True
except ImportError:
    _HMM_AVAILABLE = False
    logger.warning("hmmlearn not available — using volatility+Hurst regime only")


class RegimeState(IntEnum):
    BULL     =  1   # trending up, momentum dominates
    BEAR     = -1   # trending down, short momentum / stat arb
    SIDEWAYS =  0   # choppy, mean reversion / stat arb
    CRISIS   = -2   # vol spike, no directional trading


@dataclass
class RegimeResult:
    state:           RegimeState
    vol_regime:      str    # "low" | "normal" | "high" | "crisis"
    hurst:           float  # 0-1
    realized_vol:    float  # annualized (decimal)
    hmm_state:       int    # raw HMM state label (0, 1, or 2)
    hmm_confidence:  float  # probability of current HMM state

    # Strategy allocation weights — used by signal ensemble in main.py
    momentum_weight:  float   # [0, 1]
    mean_rev_weight:  float   # [0, 1]
    stat_arb_weight:  float   # [0, 1]
    ofi_weight:       float   # [0, 1]  (order flow always useful but varies)
    position_scale:   float   # overall position size multiplier [0, 1]


class RegimeDetector:
    """
    Per-symbol regime detector with HMM that trains online.
    Create one instance per symbol and call compute() on every eval cycle.

    sample_period_seconds: spacing between price samples.
      0.5 → live ticks (~2/s); 60 → 1-minute bars (backtest).
    """

    HURST_SCALES    = 8
    HMM_MIN_TRAIN   = 2_000
    HMM_RETRAIN_N   = 5_000

    def __init__(self, sample_period_seconds: float = 0.5):
        self.sample_period_seconds = sample_period_seconds
        # ~30 minutes of samples for realized vol
        self.VOL_WINDOW = max(30, int(1800 / sample_period_seconds))
        self._ann_factor = math.sqrt(365 * 24 * 3600 / sample_period_seconds)

        self._hmm: Optional[GaussianHMM] = None
        self._hmm_trained    = False
        self._hmm_features:  list        = []
        self._last_price: Optional[float] = None  # for incremental return appends
        self._ticks_since_retrain = 0

        self._state_vol: dict[int, float] = {}
        self._min_prices = 50 if sample_period_seconds >= 60 else 200
        self._min_rets   = 20 if sample_period_seconds >= 60 else 50

    def compute(self, prices: list[float]) -> Optional[RegimeResult]:
        if len(prices) < self._min_prices:
            return None

        arr  = np.array([p for p in prices if p > 0], dtype=float)
        rets = np.diff(arr) / arr[:-1]
        if len(rets) < self._min_rets:
            return None

        # ── 1. Realized volatility regime ────────────────────────────────
        window = min(self.VOL_WINDOW, len(rets))
        rv     = float(np.std(rets[-window:])) * self._ann_factor

        if rv > 6.0:
            vol_regime = "crisis"
        elif rv > 2.0:
            vol_regime = "high"
        elif rv > 0.5:
            vol_regime = "normal"
        else:
            vol_regime = "low"

        # ── 2. Hurst exponent (R/S analysis) ─────────────────────────────
        hurst_cap = 500 if self.sample_period_seconds >= 60 else 8000
        hurst = _compute_hurst(arr[-min(hurst_cap, len(arr)):], self.HURST_SCALES)

        # ── 3. HMM state — append only newest return by last price ────────
        hmm_state, hmm_conf = self._update_hmm_price(arr)

        # ── 4. Trend direction (price now vs 25% of window ago) ───────────
        trend_len = max(20, len(arr) // 4)
        ref_price = arr[-trend_len]
        trend_pct = (arr[-1] - ref_price) / ref_price if ref_price > 0 else 0.0

        # ── 5. Synthesize regime state ────────────────────────────────────
        state = _classify_state(vol_regime, hurst, hmm_state, hmm_conf, trend_pct)

        # ── 6. Strategy weights ───────────────────────────────────────────
        mom_w, mr_w, sa_w, ofi_w, pos_scale = _strategy_weights(state, hurst, rv)

        return RegimeResult(
            state           = state,
            vol_regime      = vol_regime,
            hurst           = round(hurst, 3),
            realized_vol    = round(rv, 4),
            hmm_state       = hmm_state,
            hmm_confidence  = round(hmm_conf, 4),
            momentum_weight = round(mom_w, 3),
            mean_rev_weight = round(mr_w, 3),
            stat_arb_weight = round(sa_w, 3),
            ofi_weight      = round(ofi_w, 3),
            position_scale  = round(pos_scale, 3),
        )

    def _update_hmm_price(self, prices: np.ndarray) -> tuple[int, float]:
        """
        Append HMM features from the newest price only.

        Using len(returns) > _hmm_n_seen freezes once a fixed-length rolling
        window fills. Tracking last price appends one return per new tick/bar.
        """
        if not _HMM_AVAILABLE or len(prices) < 2:
            return 0, 0.5

        last = float(prices[-1])
        if self._last_price is None:
            # Bootstrap with recent returns (once) without re-ingesting forever
            rets = np.diff(prices) / prices[:-1]
            seed = rets[-min(500, len(rets)):]
            feat = np.column_stack([seed, np.abs(seed)]).tolist()
            self._hmm_features.extend(feat)
            self._ticks_since_retrain += len(seed)
            self._last_price = last
        elif abs(last - self._last_price) > 1e-15:
            new_ret = (last - self._last_price) / self._last_price
            self._hmm_features.append([new_ret, abs(new_ret)])
            self._ticks_since_retrain += 1
            self._last_price = last
        # else: duplicate price — no new observation

        if len(self._hmm_features) > 30_000:
            self._hmm_features = self._hmm_features[-15_000:]

        if len(self._hmm_features) < self.HMM_MIN_TRAIN:
            return 0, 0.5

        should_retrain = (
            not self._hmm_trained
            or self._ticks_since_retrain >= self.HMM_RETRAIN_N
        )

        if should_retrain:
            self._fit_hmm()
            self._ticks_since_retrain = 0

        if not self._hmm_trained:
            return 0, 0.5

        try:
            X = np.array(self._hmm_features[-min(200, len(self._hmm_features)):])
            proba = self._hmm.predict_proba(X)
            cur   = int(np.argmax(proba[-1]))
            return cur, float(proba[-1, cur])
        except Exception:
            return 0, 0.5

    def _fit_hmm(self):
        try:
            X = np.array(self._hmm_features[-8000:])
            hmm = GaussianHMM(
                n_components=3, covariance_type="diag",
                n_iter=80, tol=1e-3, random_state=42,
            )
            hmm.fit(X)
            self._hmm         = hmm
            self._hmm_trained = True

            # Label states by mean |return| (vol proxy)
            means = hmm.means_[:, 1]   # |return| column
            self._state_vol = {i: float(means[i]) for i in range(3)}
            logger.debug(
                "HMM retrained. State vols: {}",
                {k: f"{v:.5f}" for k, v in self._state_vol.items()}
            )
        except Exception as e:
            logger.debug("HMM training failed: {}", e)


# ── Module-level helpers (pure functions) ───────────────────────────────

def _compute_hurst(prices: np.ndarray, n_scales: int = 8) -> float:
    """
    Hurst exponent via rescaled range (R/S) analysis.
    H = slope of log(R/S) vs log(lag) over multiple scales.
    """
    n = len(prices)
    if n < 64:
        return 0.5

    rs_points = []
    for i in range(3, n_scales + 3):
        lag = 2 ** i
        if lag >= n // 2:
            break
        n_chunks = n // lag
        rs_list  = []
        for c in range(n_chunks):
            chunk = prices[c * lag:(c + 1) * lag]
            mean  = np.mean(chunk)
            dev   = np.cumsum(chunk - mean)
            R     = float(np.max(dev) - np.min(dev))
            S     = float(np.std(chunk))
            if S > 1e-10:
                rs_list.append(R / S)
        if rs_list:
            rs_points.append((np.log(lag), np.log(np.mean(rs_list))))

    if len(rs_points) < 3:
        return 0.5

    x = np.array([p[0] for p in rs_points])
    y = np.array([p[1] for p in rs_points])
    h = float(np.polyfit(x, y, 1)[0])
    return float(np.clip(h, 0.0, 1.0))


def _classify_state(
    vol_regime: str, hurst: float, hmm_state: int, hmm_conf: float,
    trend_pct: float = 0.0,
) -> RegimeState:
    """
    Classify market regime using volatility, Hurst exponent, and price trend.

    trend_pct is the % price change over the most recent 25% of the window.
    This gives BULL/BEAR classification even when hmmlearn is unavailable,
    preventing the prior SIDEWAYS-only collapse that occurred with HMM off.
    """
    TREND_UP   =  0.003   # +0.3% sustained move = trending up
    TREND_DOWN = -0.003   # -0.3% sustained move = trending down

    if vol_regime == "crisis":
        return RegimeState.CRISIS

    trending_up   = trend_pct >  TREND_UP
    trending_down = trend_pct <  TREND_DOWN

    if vol_regime == "high":
        # High-vol + anti-persistent = bear crash / whipsaw
        if hurst < 0.42:
            return RegimeState.BEAR if trending_down else RegimeState.SIDEWAYS
        # High-vol + persistent: follow price direction
        if trending_up:
            return RegimeState.BULL
        if trending_down:
            return RegimeState.BEAR
        return RegimeState.SIDEWAYS

    # Normal / low vol — use trend + Hurst together
    if trending_up and hurst >= 0.48:
        return RegimeState.BULL
    if trending_down and hurst >= 0.45:
        return RegimeState.BEAR
    if hurst < 0.45:
        return RegimeState.SIDEWAYS   # mean-reverting, stat arb zone
    # Flat price, mid Hurst — genuinely sideways
    return RegimeState.SIDEWAYS


def _strategy_weights(
    state: RegimeState, hurst: float, rv: float
) -> tuple[float, float, float, float, float]:
    """Returns (momentum_w, mean_rev_w, stat_arb_w, ofi_w, position_scale)."""

    if state == RegimeState.CRISIS:
        return 0.0, 0.0, 0.0, 0.0, 0.0

    if state == RegimeState.BULL:
        # Trending up: momentum dominates, OFI confirms, stat arb is secondary
        return 0.55, 0.10, 0.20, 0.60, min(1.0, 1.5 / max(rv, 0.5))

    if state == RegimeState.BEAR:
        # Trending down: short momentum + stat arb, scale down for safety
        return 0.35, 0.15, 0.35, 0.60, min(0.8, 1.2 / max(rv, 0.5))

    # SIDEWAYS
    if hurst < 0.43:
        # Strongly mean-reverting: stat arb and OFI dominate
        return 0.10, 0.40, 0.45, 0.70, 0.75
    else:
        # Weakly trending or choppy: balanced, reduced size
        return 0.25, 0.25, 0.40, 0.65, 0.60
