"""
Consumes fill confirmations from Java (quant.executions) so the Python
paper book only updates after the broker/risk layer confirms.
"""
import json
import threading
from typing import Callable, Optional
from confluent_kafka import Consumer, KafkaError
from loguru import logger
import config


class ExecutionConsumer:
    def __init__(self, on_execution: Optional[Callable[[dict], None]] = None):
        self._consumer = Consumer({
            "bootstrap.servers":  config.KAFKA_BOOTSTRAP_SERVERS,
            "group.id":           "quant-python-executions",
            "auto.offset.reset":  "latest",
            "enable.auto.commit": True,
        })
        self._consumer.subscribe([config.TOPIC_EXECUTIONS])
        self._on_execution = on_execution
        self._running = False
        self._thread = None

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logger.info("Execution consumer started → {}", config.TOPIC_EXECUTIONS)

    def _loop(self):
        while self._running:
            try:
                msgs = self._consumer.consume(num_messages=20, timeout=0.2)
                for msg in msgs:
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            logger.error("Execution Kafka error: {}", msg.error())
                        continue
                    try:
                        data = json.loads(msg.value().decode("utf-8"))
                        if self._on_execution:
                            self._on_execution(data)
                    except Exception as e:
                        logger.error("Execution message error: {}", e)
            except Exception as e:
                logger.error("Execution consumer loop error: {}", e)

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=3)
        self._consumer.close()
