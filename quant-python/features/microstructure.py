"""
Microstructure Feature Library
================================
Pure functions for computing market microstructure metrics from raw ticks.

  - realized_vol()    — annualized close-to-close volatility
  - amihud_ratio()    — Amihud (2002) illiquidity: |ret| / volume
  - return_autocorr() — return persistence (Roll 1984 bid-ask bounce test)
  - effective_spread()— Roll (1984) implied spread from return autocovariance

These features feed the regime detector and Kelly sizer.

Amihud illiquidity is one of the most robust predictors of expected returns
across asset classes (Amihud 2002, Pastor & Stambaugh 2003). High illiquidity
→ wider spreads → larger Kelly penalty.

The Roll (1984) effective spread infers the true bid-ask cost from the serial
covariance of price changes without needing explicit bid/ask quotes.
"""

import numpy as np
from typing import Optional


def realized_vol(prices: list[float], window: int = 3600) -> float:
    """
    Annualized realized volatility from tick prices.

    Assumes ~2 ticks per second (Alpaca crypto stream typical rate).
    Returns decimal annualized vol (e.g. 0.80 = 80% annualized).
    """
    arr = np.array([p for p in prices[-window:] if p > 0], dtype=float)
    if len(arr) < 2:
        return 0.5

    rets = np.diff(arr) / arr[:-1]
    # ~2 ticks/second → annual factor = sqrt(365 * 24 * 3600 / 0.5)
    ann_factor = np.sqrt(365 * 24 * 3600 / 0.5)
    return float(np.std(rets) * ann_factor)


def amihud_ratio(ticks: list[dict], window: int = 500) -> float:
    """
    Amihud (2002) illiquidity ratio: E[|R_t| / Volume_t].

    Higher values → market is less liquid → wider true spread.
    Useful as a Kyle's lambda proxy when full order book is unavailable.
    """
    recent = ticks[-window:] if len(ticks) > window else ticks
    values = []

    for i in range(1, len(recent)):
        p0  = recent[i - 1].get("last") or recent[i - 1].get("bid", 0)
        p1  = recent[i].get("last")     or recent[i].get("bid", 0)
        vol = float(recent[i].get("volume", 0) or 0)

        if p0 > 0 and p1 > 0 and vol > 1e-8:
            abs_ret = abs((p1 - p0) / p0)
            values.append(abs_ret / vol)

    return float(np.median(values)) if values else 0.0


def return_autocorr(prices: list[float], lag: int = 1, window: int = 500) -> float:
    """
    Return autocorrelation at the given lag.

    Negative autocorrelation (bid-ask bounce) → Roll spread ≈ 2√|γ₁|
    Positive autocorrelation → momentum regime.
    Returns correlation coefficient ∈ [-1, 1].
    """
    arr = np.array([p for p in prices[-window:] if p > 0], dtype=float)
    if len(arr) < lag + 2:
        return 0.0

    rets = np.diff(arr) / arr[:-1]
    if len(rets) <= lag:
        return 0.0

    r0 = rets[:-lag]
    r1 = rets[lag:]
    cov = np.cov(r0, r1)
    denom = max(cov[0, 0] * cov[1, 1], 1e-12)
    return float(np.clip(cov[0, 1] / np.sqrt(denom), -1.0, 1.0))


def roll_effective_spread(prices: list[float], window: int = 300) -> float:
    """
    Roll (1984) implied effective spread from return serial covariance:
        c = sqrt(max(0, -Cov(r_t, r_{t-1})))
        Spread ≈ 2c

    Returns spread as fraction of price (e.g. 0.001 = 10 bps).
    """
    arr = np.array([p for p in prices[-window:] if p > 0], dtype=float)
    if len(arr) < 4:
        return 0.0

    rets  = np.diff(arr) / arr[:-1]
    cov   = float(np.cov(rets[:-1], rets[1:])[0, 1])
    c     = np.sqrt(max(0.0, -cov))
    return float(2.0 * c)


def bid_ask_bounce_fraction(ticks: list[dict], window: int = 200) -> float:
    """
    Fraction of ticks where price alternates between bid and ask.
    High fraction → noisy, market-making dominated, low signal quality.
    """
    recent = ticks[-window:] if len(ticks) > window else ticks
    if len(recent) < 3:
        return 0.0

    alternations = 0
    for i in range(1, len(recent) - 1):
        p0 = recent[i - 1].get("last") or recent[i - 1].get("bid", 0)
        p1 = recent[i].get("last")     or recent[i].get("bid", 0)
        p2 = recent[i + 1].get("last") or recent[i + 1].get("bid", 0)
        if p0 > 0 and p1 > 0 and p2 > 0:
            if (p1 > p0 and p2 < p1) or (p1 < p0 and p2 > p1):
                alternations += 1

    return alternations / max(len(recent) - 2, 1)


def compute_all(ticks: list[dict], prices: list[float]) -> dict:
    """Convenience: compute all microstructure features and return as a dict."""
    return {
        "realized_vol":         realized_vol(prices),
        "amihud":               amihud_ratio(ticks),
        "autocorr_lag1":        return_autocorr(prices, lag=1),
        "roll_spread":          roll_effective_spread(prices),
        "bounce_fraction":      bid_ask_bounce_fraction(ticks),
    }
