"""
Consumes Alpaca position snapshots from Java (quant.positions).
Broker book is the source of truth for live positions.
"""
import json
import threading
from typing import Callable, Optional
from confluent_kafka import Consumer, KafkaError
from loguru import logger
import config


class PositionConsumer:
    def __init__(self, on_positions: Optional[Callable[[dict], None]] = None):
        self._consumer = Consumer({
            "bootstrap.servers":  config.KAFKA_BOOTSTRAP_SERVERS,
            "group.id":           "quant-python-positions",
            "auto.offset.reset":  "latest",
            "enable.auto.commit": True,
        })
        self._consumer.subscribe([config.TOPIC_POSITIONS])
        self._on_positions = on_positions
        self._running = False
        self._thread = None

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logger.info("Position consumer started → {}", config.TOPIC_POSITIONS)

    def _loop(self):
        while self._running:
            try:
                msgs = self._consumer.consume(num_messages=5, timeout=0.5)
                for msg in msgs:
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            logger.error("Position Kafka error: {}", msg.error())
                        continue
                    try:
                        data = json.loads(msg.value().decode("utf-8"))
                        if self._on_positions:
                            self._on_positions(data)
                    except Exception as e:
                        logger.error("Position message error: {}", e)
            except Exception as e:
                logger.error("Position consumer loop error: {}", e)

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=3)
        self._consumer.close()
