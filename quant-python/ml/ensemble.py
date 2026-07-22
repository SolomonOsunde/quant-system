"""
Online ML ensemble with correctly paired feature/label rows per symbol.
"""
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
from sklearn.ensemble import VotingClassifier
from sklearn.model_selection import TimeSeriesSplit
from loguru import logger
import joblib
import os
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Optional


@dataclass
class MLPrediction:
    direction: float
    confidence: float
    features_used: int


class MLEnsembleModel:
    MODEL_PATH = "models/ensemble.pkl"
    MIN_TRAIN_SAMPLES = 150
    RETRAIN_EVERY_N = 200
    MAX_PENDING_PER_SYMBOL = 500
    MAX_TRAIN_ROWS = 5_000

    def __init__(self):
        self._model: Optional[Pipeline] = None
        self._sample_count = 0
        # Features waiting for their forward-return label, per symbol (FIFO)
        self._pending: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=self.MAX_PENDING_PER_SYMBOL)
        )
        self._train_X: list = []
        self._train_y: list = []
        self._is_trained = False
        self._labels_since_train = 0
        os.makedirs("models", exist_ok=True)
        self._load_model()

    def predict(self, signals: dict, price_history: list[float],
                symbol: str = "_") -> MLPrediction:
        features = self._extract_features(signals, price_history)
        if features is None:
            return MLPrediction(0.0, 0.0, 0)

        self._pending[symbol].append(features)
        self._sample_count += 1

        if not self._is_trained:
            return MLPrediction(0.0, 0.0, len(features))

        try:
            X = np.array([features])
            proba = self._model.predict_proba(X)[0]
            direction = proba[1] - proba[0]
            confidence = max(proba)
            return MLPrediction(round(direction, 4), round(confidence, 4), len(features))
        except Exception as e:
            logger.error("ML prediction error: {}", e)
            return MLPrediction(0.0, 0.0, 0)

    def label_symbol(self, symbol: str, forward_return: float):
        """
        Pair the oldest pending feature vector for this symbol with the realized
        forward return. Guarantees 1:1 feature/label alignment.
        """
        pending = self._pending.get(symbol)
        if not pending:
            return

        features = pending.popleft()
        label = 1 if forward_return > 0.0005 else 0
        self._train_X.append(features)
        self._train_y.append(label)
        self._labels_since_train += 1

        if len(self._train_X) > self.MAX_TRAIN_ROWS:
            self._train_X = self._train_X[-self.MAX_TRAIN_ROWS:]
            self._train_y = self._train_y[-self.MAX_TRAIN_ROWS:]

        if (len(self._train_y) >= self.MIN_TRAIN_SAMPLES
                and self._labels_since_train >= self.RETRAIN_EVERY_N):
            self._labels_since_train = 0
            self._train()

    def update_labels(self, future_returns: list[float]):
        """Legacy API — prefer label_symbol. Applies returns to '_' queue."""
        for ret in future_returns:
            self.label_symbol("_", ret)

    def _extract_features(self, signals: dict, prices: list[float]) -> Optional[list]:
        try:
            if len(prices) < 10:
                return None

            p = np.array(prices[-20:])
            returns = np.diff(p) / p[:-1]

            return [
                signals.get("rsi", 50) / 100,
                signals.get("macd_hist", 0),
                signals.get("bb_pct_b", 0.5),
                signals.get("ema_cross", 0),
                signals.get("atr_pct", 0.1) / 100,
                signals.get("volume_ratio", 1.0),
                signals.get("score", 0),
                returns[-1] if len(returns) >= 1 else 0,
                returns[-3:].mean() if len(returns) >= 3 else 0,
                returns[-5:].mean() if len(returns) >= 5 else 0,
                returns[-10:].std() if len(returns) >= 10 else 0,
                (p[-1] - p[-5]) / p[-5] if p[-5] > 0 else 0,
            ]
        except Exception as e:
            logger.warning("Feature extraction error: {}", e)
            return None

    def _train(self):
        n = min(len(self._train_X), len(self._train_y))
        if n < self.MIN_TRAIN_SAMPLES:
            return

        X = np.array(self._train_X[:n])
        y = np.array(self._train_y[:n])
        logger.info("Training ML ensemble on {} paired samples...", n)

        xgb = XGBClassifier(
            n_estimators=200, max_depth=4, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8,
            eval_metric="logloss", verbosity=0,
        )
        lgbm = LGBMClassifier(
            n_estimators=200, max_depth=4, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8, verbose=-1,
        )
        ensemble = VotingClassifier(
            estimators=[("xgb", xgb), ("lgbm", lgbm)],
            voting="soft", weights=[1, 1],
        )
        self._model = Pipeline([
            ("scaler", StandardScaler()),
            ("ensemble", ensemble),
        ])

        tscv = TimeSeriesSplit(n_splits=3)
        scores = []
        for train_idx, val_idx in tscv.split(X):
            self._model.fit(X[train_idx], y[train_idx])
            scores.append(self._model.score(X[val_idx], y[val_idx]))

        self._model.fit(X, y)
        self._is_trained = True
        logger.info("ML ensemble trained. CV accuracy: {:.3f} ± {:.3f}",
                    np.mean(scores), np.std(scores))
        self._save_model()

    def _save_model(self):
        try:
            joblib.dump(self._model, self.MODEL_PATH)
        except Exception as e:
            logger.warning("Could not save model: {}", e)

    def _load_model(self):
        if os.path.exists(self.MODEL_PATH):
            try:
                self._model = joblib.load(self.MODEL_PATH)
                self._is_trained = True
                logger.info("ML model loaded from {}", self.MODEL_PATH)
            except Exception as e:
                logger.warning("Could not load model: {}", e)
