import numpy as np
import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import MACD, EMAIndicator
from ta.volatility import BollingerBands, AverageTrueRange
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
    Multi-factor technical signals from tick data.

    Uses shortened indicator periods so signals fire within ~5 min of startup:
      - RSI (10)              — momentum
      - MACD (10/21/7)        — trend  (slow=21 bars × 15s = 5.25 min warmup)
      - Bollinger Bands (15/2) — mean reversion
      - EMA crossover (7/15)  — trend confirmation
      - ATR (10)              — volatility filter
      - Volume spike          — confirmation
    """

    def compute(self, symbol: str, ticks: list[dict]) -> Optional[SignalResult]:
        if len(ticks) < 30:
            return None

        df = self._ticks_to_ohlcv(ticks)
        if df is None or len(df) < 5:
            return None

        try:
            return self._compute_signals(symbol, df)
        except Exception as e:
            logger.error("Signal computation error for {}: {}", symbol, e)
            return None

    def _ticks_to_ohlcv(self, ticks: list[dict]) -> Optional[pd.DataFrame]:
        """Resample raw ticks into 15-second OHLCV bars."""
        try:
            df = pd.DataFrame(ticks)
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            df = df.set_index("timestamp").sort_index()
            df["price"] = df["last"].fillna(df.get("bid", 0))

            ohlcv = df["price"].resample("15s").ohlc()
            ohlcv["volume"] = df["volume"].resample("15s").sum()
            ohlcv = ohlcv.dropna()
            return ohlcv if len(ohlcv) >= 5 else None

        except Exception as e:
            logger.warning("OHLCV conversion error: {}", e)
            return None

    def _compute_signals(self, symbol: str, df: pd.DataFrame) -> SignalResult:
        close  = df["close"]
        high   = df["high"]
        low    = df["low"]
        volume = df["volume"]

        signals: dict = {}
        scores:  list = []

        # ── RSI (5) ────────────────────────────────────────────────────────
        try:
            rsi_val = RSIIndicator(close=close, window=5).rsi().iloc[-1]
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
        except Exception:
            pass

        # ── MACD (5/10/4) ─────────────────────────────────────────────────
        try:
            macd_obj  = MACD(close=close, window_fast=5, window_slow=10, window_sign=4)
            macd_line = macd_obj.macd().iloc[-1]
            macd_sig  = macd_obj.macd_signal().iloc[-1]
            macd_hist = macd_obj.macd_diff().iloc[-1]
            signals["macd_hist"] = round(macd_hist, 6)
            if macd_line > macd_sig and macd_hist > 0:
                scores.append(+0.8)
            elif macd_line < macd_sig and macd_hist < 0:
                scores.append(-0.8)
            else:
                scores.append(0.0)
        except Exception:
            pass

        # ── Bollinger Bands (8/2) ─────────────────────────────────────────
        try:
            bb     = BollingerBands(close=close, window=8, window_dev=2)
            upper  = bb.bollinger_hband().iloc[-1]
            lower  = bb.bollinger_lband().iloc[-1]
            price  = close.iloc[-1]
            pct_b  = (price - lower) / (upper - lower) if (upper - lower) > 0 else 0.5
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
        except Exception:
            pass

        # ── EMA crossover (4/8) ───────────────────────────────────────────
        try:
            e4  = EMAIndicator(close=close, window=4).ema_indicator().iloc[-1]
            e8  = EMAIndicator(close=close, window=8).ema_indicator().iloc[-1]
            signals["ema_cross"] = round(e4 - e8, 6)
            scores.append(+0.6 if e4 > e8 else -0.6)
        except Exception:
            pass

        # ── ATR volatility filter (5) ─────────────────────────────────────
        try:
            atr_val = AverageTrueRange(
                high=high, low=low, close=close, window=5
            ).average_true_range().iloc[-1]
            atr_pct = atr_val / close.iloc[-1] if close.iloc[-1] > 0 else 0
            signals["atr_pct"] = round(atr_pct * 100, 4)
            if atr_pct > 0.03:
                scores = [s * 0.5 for s in scores]
            elif atr_pct < 0.0001:
                scores = [s * 0.3 for s in scores]
        except Exception:
            pass

        # ── Volume confirmation ───────────────────────────────────────────
        try:
            if len(volume) >= 15:
                avg_vol  = volume.iloc[-15:].mean()
                last_vol = volume.iloc[-1]
                vol_ratio = last_vol / avg_vol if avg_vol > 0 else 1.0
                signals["volume_ratio"] = round(vol_ratio, 2)
                if vol_ratio > 1.5:
                    scores = [s * 1.2 for s in scores]
        except Exception:
            pass

        # ── Aggregate ─────────────────────────────────────────────────────
        if not scores:
            return SignalResult(symbol, 0, 0.0, signals, "Insufficient data")

        avg_score  = float(np.mean(scores))
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
