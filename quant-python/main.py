"""
Quant System — Python AI Layer
Consumes ticks from Java via Kafka, computes signals,
runs ML ensemble + Claude AI reasoning, publishes trade signals.
"""
import time
import threading
from collections import defaultdict
from loguru import logger

import config
from kafka.tick_consumer import TickConsumer
from kafka.signal_producer import SignalProducer
from signals.technical import TechnicalSignalEngine
from ml.ensemble import MLEnsembleModel
from ai.claude_engine import ClaudeReasoningEngine
from dashboard.app import start_dashboard


class QuantPythonLayer:

    SIGNAL_INTERVAL_SEC = 5    # evaluate signals every N seconds per symbol
    MIN_TICKS_REQUIRED  = 60

    def __init__(self):
        logger.info("=== Polyglot Quant System — Python AI Layer Starting ===")

        self.tick_consumer  = TickConsumer(on_tick=self._on_tick)
        self.signal_producer = SignalProducer()
        self.tech_engine    = TechnicalSignalEngine()
        self.ml_model       = MLEnsembleModel()
        self.claude_engine  = ClaudeReasoningEngine()

        self._last_signal_time: dict[str, float] = defaultdict(float)
        self._positions: dict[str, float]        = defaultdict(float)
        self._pnl_history: list[dict]            = []
        self._lock = threading.Lock()

    def _on_tick(self, tick: dict):
        """Called on every incoming tick — lightweight, just buffers."""
        pass  # tick buffering handled by TickConsumer

    def _evaluate_symbol(self, symbol: str):
        """Full signal evaluation pipeline for one symbol."""
        now = time.time()

        # Rate limit per symbol
        if now - self._last_signal_time[symbol] < self.SIGNAL_INTERVAL_SEC:
            return

        ticks = self.tick_consumer.get_ticks(symbol)
        if len(ticks) < self.MIN_TICKS_REQUIRED:
            return

        current_tick = ticks[-1]
        market       = current_tick.get("market", "UNKNOWN")
        price        = current_tick.get("last") or current_tick.get("bid", 0)
        if price <= 0:
            return

        # Compute spread
        bid = current_tick.get("bid", price)
        ask = current_tick.get("ask", price)
        spread_bps = ((ask - bid) / price * 10_000) if price > 0 else 0

        # Skip if spread too wide
        if spread_bps > config.MAX_SPREAD_BPS:
            return

        # 1. Technical signals
        tech_result = self.tech_engine.compute(symbol, ticks)
        if tech_result is None:
            return

        # 2. ML ensemble prediction
        prices = [t.get("last", t.get("bid", 0)) for t in ticks]
        ml_pred = self.ml_model.predict(tech_result.signals, prices)

        # Quick filter: skip if both are neutral
        if tech_result.direction == 0 and abs(ml_pred.direction) < 0.3:
            return

        # 3. Claude AI reasoning
        claude_decision = self.claude_engine.reason(
            symbol          = symbol,
            market          = market,
            technical_signals   = tech_result.signals,
            technical_direction = tech_result.direction,
            technical_confidence= tech_result.confidence,
            ml_prediction   = ml_pred.direction,
            ml_confidence   = ml_pred.confidence,
            current_price   = price,
            spread_bps      = spread_bps,
        )

        # 4. Final decision
        decision   = claude_decision.get("decision", "HOLD")
        confidence = claude_decision.get("confidence", 0.0)
        reasoning  = claude_decision.get("reasoning", "")
        size_pct   = claude_decision.get("suggested_size_pct", 0.5)

        if decision in ("BUY", "SELL") and confidence >= config.MIN_CONFIDENCE:
            quantity = self._size_position(price, size_pct, market)

            published = self.signal_producer.publish_signal(
                symbol     = symbol,
                side       = decision,
                quantity   = quantity,
                price      = price,
                order_type = "MARKET",
                strategy   = "POLYGLOT_QUANT_V1",
                confidence = confidence,
                spread_bps = spread_bps,
                reasoning  = reasoning,
            )
            if published:
                self._last_signal_time[symbol] = now

    def _size_position(self, price: float, size_pct: float, market: str) -> float:
        """Kelly-inspired position sizing."""
        max_usd  = config.MAX_ORDER_USD * size_pct
        quantity = max_usd / price if price > 0 else 0

        # Round to market conventions
        if market == "CRYPTO":
            return round(quantity, 6)
        elif market == "FOREX":
            return round(quantity * 1000) * 1000   # lot sizing
        else:
            return max(1, round(quantity))

    def run(self):
        """Main loop — starts all components."""
        self.tick_consumer.start()
        logger.info("Waiting for market data...")

        # Start dashboard in background thread
        dashboard_thread = threading.Thread(
            target=start_dashboard,
            kwargs={"pnl_source": self._pnl_history},
            daemon=True
        )
        dashboard_thread.start()

        try:
            while True:
                symbols = self.tick_consumer.get_symbols()
                for symbol in symbols:
                    try:
                        self._evaluate_symbol(symbol)
                    except Exception as e:
                        logger.error("Error evaluating {}: {}", symbol, e)
                time.sleep(1)

        except KeyboardInterrupt:
            logger.info("Shutting down Python AI layer...")
        finally:
            self.tick_consumer.stop()
            self.signal_producer.flush()
            logger.info("Python AI layer stopped.")


if __name__ == "__main__":
    QuantPythonLayer().run()
