"""
Online slippage calibration from broker fills vs signal mid.

Stores rolling samples in Redis `quant:slippage_calib` and exposes a
multiplicative stress scale for the forward slip model.
"""
from __future__ import annotations

import json
import threading
import time
from collections import deque
from typing import Optional

from loguru import logger

import config
from ops.alerts import alerts

try:
    import redis as _redis
except ImportError:
    _redis = None


class SlippageCalibrator:
    MAX_SAMPLES = 200
    BIAS_ALERT_BPS = 25.0

    def __init__(self):
        self._lock = threading.Lock()
        self._samples: deque[dict] = deque(maxlen=self.MAX_SAMPLES)
        self._redis = None
        self._connect()

    def _connect(self):
        if _redis is None:
            return
        try:
            r = _redis.Redis(
                host=config.REDIS_HOST, port=config.REDIS_PORT,
                decode_responses=True, socket_timeout=2,
            )
            r.ping()
            self._redis = r
        except Exception:
            self._redis = None

    def record(
        self,
        symbol: str,
        side: str,
        signal_price: float,
        fill_price: float,
        *,
        model_slip_bps: float = 0.0,
    ) -> Optional[float]:
        """
        Record actual vs signal. Returns signed actual slip bps
        (positive = adverse for the trade).
        """
        if signal_price <= 0 or fill_price <= 0:
            return None
        side_u = (side or "").upper()
        if side_u == "BUY":
            actual_bps = (fill_price - signal_price) / signal_price * 10_000
        else:
            actual_bps = (signal_price - fill_price) / signal_price * 10_000

        sample = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "symbol": symbol,
            "side": side_u,
            "signal_price": round(signal_price, 8),
            "fill_price": round(fill_price, 8),
            "actual_bps": round(actual_bps, 3),
            "model_bps": round(model_slip_bps, 3),
        }
        with self._lock:
            self._samples.append(sample)
            mean_bias = self._mean_bias_locked()

        if self._redis:
            try:
                self._redis.lpush("quant:slippage_calib", json.dumps(sample))
                self._redis.ltrim("quant:slippage_calib", 0, self.MAX_SAMPLES - 1)
                self._redis.set("quant:slippage_bias_bps", f"{mean_bias:.3f}")
            except Exception:
                pass

        if abs(mean_bias) >= self.BIAS_ALERT_BPS and len(self._samples) >= 20:
            alerts.emit(
                "WARN", "SLIP_BIAS",
                f"Rolling fill slip bias {mean_bias:.1f} bps",
                bias_bps=mean_bias, n=len(self._samples),
            )

        logger.debug(
            "Slip calib {} {} actual={:.1f}bps model={:.1f}bps bias={:.1f}",
            side_u, symbol, actual_bps, model_slip_bps, mean_bias,
        )
        return actual_bps

    def _mean_bias_locked(self) -> float:
        if not self._samples:
            return 0.0
        return sum(s["actual_bps"] for s in self._samples) / len(self._samples)

    def mean_bias_bps(self) -> float:
        with self._lock:
            return self._mean_bias_locked()

    def stress_scale(self) -> float:
        """
        Scale forward stress model: if fills are worse than model, inflate.
        Clipped to [0.8, 2.0].
        """
        bias = self.mean_bias_bps()
        # Each 10 bps adverse bias → +10% model scale
        scale = 1.0 + max(0.0, bias) / 100.0
        return float(max(0.8, min(2.0, scale)))


slip_calibrator = SlippageCalibrator()
