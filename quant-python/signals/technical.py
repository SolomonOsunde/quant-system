import pandas as pd
import pandas_ta as ta
import numpy as np
from loguru import logger
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class SignalResult:
    symbol:     str
    direction:  int          # +1 BUY, -1 SELL, 0 NEUTRAL
    confidence: float        # 0.0 – 1.0
    signals:    dict = field(default_factory=dict)
    reason:     str  = ""


class TechnicalSignalEngine:
    """
    Computes multi-factor technical signals from tick data.
    Returns a SignalResult with direction and confidence score.

    Indicators used:
      - RSI (14) — momentum
      - MACD (12/26/9) — trend
      - Bollinger Bands (20/2) — mean reversion
      - ATR (14) — volatility filter
      - VWAP — intraday fair value
      - EMA crossover (9/21) — trend confirmation
      - Volume spike — confirmation
    """

    def compute(self, symbol: str, ticks: list[dict]) -> Optional[SignalResult]:
        if len(ticks) < 50:
            return None

        df = self._ticks_to_ohlcv(ticks)
        if df is None or len(df) < 30:
            return None

        try:
            return self._compute_signals(symbol, df)
        except Exception as e:
            logger.error("Signal computation error for {}: {}", symbol, e)
            return None

    def _ticks_to_ohlcv(self, ticks: list[dict]) -> Optional[pd.DataFrame]:
        """Resample raw ticks into 1-minute OHLCV bars."""
        try:
            df = pd.DataFrame(ticks)
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            df = df.set_index("timestamp").sort_index()
            df["price"] = df["last"].fillna(df.get("bid", 0))

            ohlcv = df["price"].resample("1min").ohlc()
            ohlcv["volume"] = df["volume"].resample("1min").sum()
            ohlcv = ohlcv.dropna()
            return ohlcv if len(ohlcv) >= 20 else None

        except Exception as e:
            logger.warning("OHLCV conversion error: {}", e)
            return None

    def _compute_signals(self, symbol: str, df: pd.DataFrame) -> SignalResult:
        close  = df["close"]
        high   = df["high"]
        low    = df["low"]
        volume = df["volume"]

        signals = {}
        scores  = []

        # --- RSI ---
        rsi = ta.rsi(close, length=14)
        if rsi is not None and not rsi.empty:
            rsi_val = rsi.iloc[-1]
            signals["rsi"] = round(rsi_val, 2)
            if rsi_val < 30:
                scores.append(+1.0)
            elif rsi_val > 70:
                scores.append(-1.0)
            elif rsi_val < 45:
                scores.append(+0.4)
            elif rsi_val > 55:
                scores.append(-0.4)
            else:
                scores.append(0.0)

        # --- MACD ---
        macd = ta.macd(close, fast=12, slow=26, signal=9)
        if macd is not None and not macd.empty:
            macd_line = macd["MACD_12_26_9"].iloc[-1]
            macd_sig  = macd["MACDs_12_26_9"].iloc[-1]
            macd_hist = macd["MACDh_12_26_9"].iloc[-1]
            signals["macd_hist"] = round(macd_hist, 6)
            if macd_line > macd_sig and macd_hist > 0:
                scores.append(+0.8)
            elif macd_line < macd_sig and macd_hist < 0:
                scores.append(-0.8)
            else:
                scores.append(0.0)

        # --- Bollinger Bands ---
        bb = ta.bbands(close, length=20, std=2)
        if bb is not None and not bb.empty:
            upper = bb["BBU_20_2.0"].iloc[-1]
            lower = bb["BBL_20_2.0"].iloc[-1]
            mid   = bb["BBM_20_2.0"].iloc[-1]
            price = close.iloc[-1]
            pct_b = (price - lower) / (upper - lower) if (upper - lower) > 0 else 0.5
            signals["bb_pct_b"] = round(pct_b, 4)
            if pct_b < 0.05:
                scores.append(+0.9)
            elif pct_b > 0.95:
                scores.append(-0.9)
            elif pct_b < 0.25:
                scores.append(+0.3)
            elif pct_b > 0.75:
                scores.append(-0.3)
            else:
                scores.append(0.0)

        # --- EMA crossover (9/21) ---
        ema9  = ta.ema(close, length=9)
        ema21 = ta.ema(close, length=21)
        if ema9 is not None and ema21 is not None:
            e9, e21 = ema9.iloc[-1], ema21.iloc[-1]
            signals["ema_cross"] = round(e9 - e21, 6)
            if e9 > e21:
                scores.append(+0.6)
            else:
                scores.append(-0.6)

        # --- ATR volatility filter ---
        atr = ta.atr(high, low, close, length=14)
        if atr is not None and not atr.empty:
            atr_val  = atr.iloc[-1]
            atr_pct  = atr_val / close.iloc[-1] if close.iloc[-1] > 0 else 0
            signals["atr_pct"] = round(atr_pct * 100, 4)
            # Filter: skip signals in extremely high or low volatility
            if atr_pct > 0.03:   # >3% ATR — too volatile
                scores = [s * 0.5 for s in scores]
            elif atr_pct < 0.0001:  # near-zero ATR — no movement
                scores = [s * 0.3 for s in scores]

        # --- Volume confirmation ---
        if len(volume) >= 20:
            avg_vol  = volume.iloc[-20:].mean()
            last_vol = volume.iloc[-1]
            vol_ratio = last_vol / avg_vol if avg_vol > 0 else 1.0
            signals["volume_ratio"] = round(vol_ratio, 2)
            if vol_ratio > 1.5:
                scores = [s * 1.2 for s in scores]  # boost on high volume

        # --- Aggregate ---
        if not scores:
            return SignalResult(symbol, 0, 0.0, signals, "Insufficient data")

        avg_score  = np.mean(scores)
        confidence = min(abs(avg_score), 1.0)
        direction  = +1 if avg_score > 0.15 else (-1 if avg_score < -0.15 else 0)

        reason = (
            f"RSI={signals.get('rsi', 'N/A')} "
            f"MACD_hist={signals.get('macd_hist', 'N/A')} "
            f"BB%B={signals.get('bb_pct_b', 'N/A')} "
            f"score={avg_score:.3f}"
        )

        signals["price"] = round(close.iloc[-1], 6)
        signals["score"] = round(avg_score, 4)

        return SignalResult(symbol, direction, confidence, signals, reason)
