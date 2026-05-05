import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
from sklearn.ensemble import VotingClassifier
from sklearn.model_selection import TimeSeriesSplit
from loguru import logger
import joblib
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class MLPrediction:
    direction: float    # +1 BUY, -1 SELL (probability-weighted)
    confidence: float   # 0.0 – 1.0
    features_used: int


class MLEnsembleModel:
    """
    XGBoost + LightGBM voting ensemble for trade signal prediction.

    Features:
      - RSI, MACD, Bollinger %B, EMA cross
      - ATR, volume ratio
      - Lagged returns (1, 3, 5 bars)
      - Rolling volatility

    Labels:
      - 1 = price up >0.05% in next 5 bars
      - 0 = price flat or down
    """

    MODEL_PATH = "models/ensemble.pkl"
    MIN_TRAIN_SAMPLES = 500
    RETRAIN_EVERY_N   = 1000   # retrain after N new samples

    def __init__(self):
        self._model: Optional[Pipeline] = None
        self._sample_count = 0
        self._feature_buffer = []
        self._label_buffer   = []
        self._is_trained     = False
        os.makedirs("models", exist_ok=True)
        self._load_model()

    def predict(self, signals: dict, price_history: list[float]) -> MLPrediction:
        """Predict trade direction from computed signals."""
        features = self._extract_features(signals, price_history)
        if features is None:
            return MLPrediction(0.0, 0.0, 0)

        # Buffer for online learning
        self._feature_buffer.append(features)
        self._sample_count += 1

        if not self._is_trained:
            return MLPrediction(0.0, 0.0, len(features))

        try:
            X = np.array([features])
            proba = self._model.predict_proba(X)[0]
            # proba[1] = P(price up)
            direction  = proba[1] - proba[0]   # range -1 to +1
            confidence = max(proba)
            return MLPrediction(round(direction, 4), round(confidence, 4), len(features))

        except Exception as e:
            logger.error("ML prediction error: {}", e)
            return MLPrediction(0.0, 0.0, 0)

    def update_labels(self, future_returns: list[float]):
        """
        Called after N bars to provide labels for buffered features.
        Labels: 1 if return > +0.05%, else 0.
        """
        for ret in future_returns:
            self._label_buffer.append(1 if ret > 0.0005 else 0)

        # Retrain periodically
        if (len(self._label_buffer) >= self.MIN_TRAIN_SAMPLES and
                self._sample_count % self.RETRAIN_EVERY_N == 0):
            self._train()

    def _extract_features(self, signals: dict, prices: list[float]) -> Optional[list]:
        """Extract feature vector from signals dict + price history."""
        try:
            if len(prices) < 10:
                return None

            p = np.array(prices[-20:])
            returns = np.diff(p) / p[:-1]

            features = [
                signals.get("rsi", 50) / 100,
                signals.get("macd_hist", 0),
                signals.get("bb_pct_b", 0.5),
                signals.get("ema_cross", 0),
                signals.get("atr_pct", 0.1) / 100,
                signals.get("volume_ratio", 1.0),
                signals.get("score", 0),
                # Lagged returns
                returns[-1] if len(returns) >= 1 else 0,
                returns[-3:].mean() if len(returns) >= 3 else 0,
                returns[-5:].mean() if len(returns) >= 5 else 0,
                # Rolling volatility
                returns[-10:].std() if len(returns) >= 10 else 0,
                # Price momentum
                (p[-1] - p[-5]) / p[-5] if p[-5] > 0 else 0,
            ]
            return features

        except Exception as e:
            logger.warning("Feature extraction error: {}", e)
            return None

    def _train(self):
        """Train the ensemble on buffered data."""
        min_len = min(len(self._feature_buffer), len(self._label_buffer))
        if min_len < self.MIN_TRAIN_SAMPLES:
            return

        X = np.array(self._feature_buffer[:min_len])
        y = np.array(self._label_buffer[:min_len])

        logger.info("Training ML ensemble on {} samples...", min_len)

        xgb = XGBClassifier(
            n_estimators=200, max_depth=4, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8,
            eval_metric="logloss", verbosity=0
        )
        lgbm = LGBMClassifier(
            n_estimators=200, max_depth=4, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8, verbose=-1
        )

        ensemble = VotingClassifier(
            estimators=[("xgb", xgb), ("lgbm", lgbm)],
            voting="soft", weights=[1, 1]
        )

        self._model = Pipeline([
            ("scaler",   StandardScaler()),
            ("ensemble", ensemble),
        ])

        # Time-series cross-validation
        tscv = TimeSeriesSplit(n_splits=3)
        scores = []
        for train_idx, val_idx in tscv.split(X):
            self._model.fit(X[train_idx], y[train_idx])
            score = self._model.score(X[val_idx], y[val_idx])
            scores.append(score)

        # Final fit on all data
        self._model.fit(X, y)
        self._is_trained = True

        logger.info("ML ensemble trained. CV accuracy: {:.3f} ± {:.3f}",
                    np.mean(scores), np.std(scores))
        self._save_model()

    def _save_model(self):
        try:
            joblib.dump(self._model, self.MODEL_PATH)
            logger.info("ML model saved to {}", self.MODEL_PATH)
        except Exception as e:
            logger.warning("Could not save model: {}", e)

    def _load_model(self):
        if os.path.exists(self.MODEL_PATH):
            try:
                self._model    = joblib.load(self.MODEL_PATH)
                self._is_trained = True
                logger.info("ML model loaded from {}", self.MODEL_PATH)
            except Exception as e:
                logger.warning("Could not load model: {}", e)
