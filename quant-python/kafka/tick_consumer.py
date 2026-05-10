import json
import threading
from collections import defaultdict, deque
from typing import Callable
from confluent_kafka import Consumer, KafkaError
from loguru import logger
import config


class TickConsumer:
    """
    Consumes raw tick data from the Java core via Kafka.
    Maintains a rolling window of ticks per symbol for signal computation.
    """

    WINDOW_SIZE = 15_000  # ~5 min of BTC at 50 ticks/sec; ~2 hrs commodity at 2/sec

    def __init__(self, on_tick: Callable[[dict], None] = None):
        self._consumer = Consumer({
            "bootstrap.servers":  config.KAFKA_BOOTSTRAP_SERVERS,
            "group.id":           "quant-python-signal-engine",
            "auto.offset.reset":  "latest",
            "enable.auto.commit": True,
        })
        self._consumer.subscribe([config.TOPIC_TICKS])
        self._tick_windows: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=self.WINDOW_SIZE)
        )
        self._on_tick = on_tick
        self._running = False
        self._thread  = None
        self._lock    = threading.Lock()

    def start(self):
        self._running = True
        self._thread  = threading.Thread(target=self._consume_loop, daemon=True)
        self._thread.start()
        logger.info("Tick consumer started → {}", config.KAFKA_BOOTSTRAP_SERVERS)

    def _consume_loop(self):
        while self._running:
            try:
                msgs = self._consumer.consume(num_messages=50, timeout=0.1)
                for msg in msgs:
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            logger.error("Kafka error: {}", msg.error())
                        continue
                    self._process_message(msg)
            except Exception as e:
                logger.error("Consumer loop error: {}", e)

    def _process_message(self, msg):
        try:
            tick = json.loads(msg.value().decode("utf-8"))
            symbol = tick.get("symbol", "")
            if not symbol:
                return

            with self._lock:
                self._tick_windows[symbol].append(tick)

            if self._on_tick:
                self._on_tick(tick)

        except json.JSONDecodeError as e:
            logger.warning("JSON decode error: {}", e)

    def get_ticks(self, symbol: str) -> list[dict]:
        """Return all buffered ticks for a symbol (thread-safe)."""
        with self._lock:
            return list(self._tick_windows.get(symbol, []))

    def get_symbols(self) -> list[str]:
        with self._lock:
            return list(self._tick_windows.keys())

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        self._consumer.close()
        logger.info("Tick consumer stopped.")
