import json
import time
from confluent_kafka import Producer
from loguru import logger
import config


class SignalProducer:
    """
    Publishes trade signals from Python AI layer back to Java execution engine.
    """

    def __init__(self):
        self._producer = Producer({
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "acks":              "1",
            "linger.ms":         "1",
            "compression.type":  "lz4",
        })
        self._signal_count = 0
        logger.info("Signal producer initialized.")

    def publish_signal(
        self,
        symbol: str,
        side: str,           # BUY or SELL
        quantity: float,
        price: float,
        order_type: str = "MARKET",
        strategy: str = "ML_ENSEMBLE",
        confidence: float = 0.0,
        spread_bps: float = 0.0,
        reasoning: str = "",
        simulation: bool = False,
        is_exit: bool = False,
        entry_price: float = 0.0,
        pnl: float = None,
    ) -> bool:
        """
        Publish a trade signal to the Java execution engine.
        Returns True if published successfully.
        """
        signal = {
            "symbol":     symbol,
            "side":       side.upper(),
            "quantity":   round(quantity, 8),
            "price":      round(price, 8),
            "orderType":  order_type,
            "strategy":   strategy,
            "confidence": round(confidence, 4),
            "spreadBps":  round(spread_bps, 2),
            "reasoning":  reasoning[:500],
            "timestamp":  int(time.time() * 1000),
            "simulation": simulation,
            "isExit":     is_exit,
        }
        if entry_price > 0:
            signal["entryPrice"] = round(entry_price, 8)
        if pnl is not None:
            signal["pnl"] = round(pnl, 4)

        try:
            self._producer.produce(
                topic=config.TOPIC_SIGNALS,
                key=symbol.encode(),
                value=json.dumps(signal).encode(),
                callback=self._delivery_callback,
            )
            self._producer.poll(0)
            self._signal_count += 1
            logger.info("Signal: {} {} {} @ {:.5f} | conf={:.2f} strategy={} exit={}",
                        side, quantity, symbol, price, confidence, strategy, is_exit)
            return True

        except Exception as e:
            logger.error("Failed to publish signal for {}: {}", symbol, e)
            return False

    def _delivery_callback(self, err, msg):
        if err:
            logger.error("Signal delivery failed: {}", err)

    def flush(self):
        self._producer.flush()

    @property
    def signal_count(self) -> int:
        return self._signal_count
