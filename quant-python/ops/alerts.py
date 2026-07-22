"""
Ops alerting — structured events for breaches, fill failures, desync, and health.

Sinks: loguru + Redis list `quant:alerts` + optional webhook (ALERT_WEBHOOK_URL).
"""
from __future__ import annotations

import json
import time
import threading
from loguru import logger

import config

try:
    import redis as _redis
except ImportError:
    _redis = None


class AlertManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._recent: list[dict] = []
        self._redis = None
        self._webhook = getattr(config, "ALERT_WEBHOOK_URL", "") or ""
        self._connect_redis()

    def _connect_redis(self):
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

    def emit(self, level: str, code: str, message: str, **ctx):
        level = (level or "INFO").upper()
        event = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "level": level,
            "code": code,
            "message": message,
            "ctx": {k: _jsonable(v) for k, v in ctx.items()},
        }
        with self._lock:
            self._recent.append(event)
            if len(self._recent) > 200:
                self._recent = self._recent[-200:]

        log_fn = {
            "INFO": logger.info,
            "WARN": logger.warning,
            "ERROR": logger.error,
            "CRITICAL": logger.error,
        }.get(level, logger.info)
        log_fn("ALERT [{}] {}: {} {}", level, code, message, ctx or "")

        if self._redis:
            try:
                self._redis.lpush("quant:alerts", json.dumps(event))
                self._redis.ltrim("quant:alerts", 0, 99)
            except Exception:
                pass

        if self._webhook and level in ("ERROR", "CRITICAL", "WARN"):
            self._post_webhook(event)

    def _post_webhook(self, event: dict):
        try:
            import urllib.request
            req = urllib.request.Request(
                self._webhook,
                data=json.dumps(event).encode(),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=3)
        except Exception as e:
            logger.debug("Alert webhook failed: {}", e)

    def recent(self, n: int = 20) -> list[dict]:
        with self._lock:
            return list(self._recent[-n:])


def _jsonable(v):
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    return str(v)


alerts = AlertManager()
