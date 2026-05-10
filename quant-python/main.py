"""
Quant System — Python AI Layer
Consumes ticks from Java via Kafka, computes signals,
runs ML ensemble + Claude AI reasoning, publishes trade signals,
and tracks paper trading P&L for the live dashboard.
"""
import json
import time
import threading
from collections import defaultdict, deque
from loguru import logger
import redis

import config
from kafka.tick_consumer import TickConsumer
from kafka.signal_producer import SignalProducer
from signals.technical import TechnicalSignalEngine
from ml.ensemble import MLEnsembleModel
from ai.claude_engine import ClaudeReasoningEngine
from dashboard.app import start_dashboard


class PaperPortfolio:
    """
    Simulates order fills and tracks positions + P&L.
    Thread-safe via internal lock.
    """

    def __init__(self, initial_capital: float):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self._positions: dict[str, dict] = {}
        self._realized_pnl = 0.0
        self._lock = threading.Lock()

    def fill(self, symbol: str, side: str, quantity: float, price: float, market: str):
        """Simulate an order fill at the given price."""
        cost = quantity * price
        with self._lock:
            pos = self._positions.get(symbol, {
                "qty": 0.0, "avg_price": 0.0, "side": "FLAT", "market": market
            })

            if side == "BUY":
                if pos["side"] == "SELL" and pos["qty"] > 0:
                    # Close existing short
                    self._realized_pnl += pos["qty"] * (pos["avg_price"] - price)
                    pos = {"qty": 0.0, "avg_price": 0.0, "side": "FLAT", "market": market}
                else:
                    # Open or add to long
                    total_qty = pos["qty"] + quantity
                    pos["avg_price"] = (
                        (pos["qty"] * pos.get("avg_price", price) + cost) / total_qty
                    )
                    pos["qty"] = total_qty
                    pos["side"] = "BUY"
                self.cash -= cost

            elif side == "SELL":
                if pos["side"] == "BUY" and pos["qty"] > 0:
                    # Close existing long
                    self._realized_pnl += pos["qty"] * (price - pos["avg_price"])
                    pos = {"qty": 0.0, "avg_price": 0.0, "side": "FLAT", "market": market}
                else:
                    # Open or add to short
                    total_qty = pos["qty"] + quantity
                    pos["avg_price"] = (
                        (pos["qty"] * pos.get("avg_price", price) + cost) / total_qty
                    )
                    pos["qty"] = total_qty
                    pos["side"] = "SELL"
                self.cash += cost

            pos["market"] = market
            self._positions[symbol] = pos

    def net_pnl(self, current_prices: dict) -> float:
        """Total P&L = realized + unrealized, relative to initial capital."""
        unrealized = 0.0
        with self._lock:
            for symbol, pos in self._positions.items():
                if pos["qty"] <= 0:
                    continue
                price = current_prices.get(symbol, pos["avg_price"])
                if pos["side"] == "BUY":
                    unrealized += pos["qty"] * (price - pos["avg_price"])
                elif pos["side"] == "SELL":
                    unrealized += pos["qty"] * (pos["avg_price"] - price)
            return round(self._realized_pnl + unrealized, 2)

    def positions_snapshot(self, current_prices: dict) -> list[dict]:
        """Return open positions with live mark-to-market P&L."""
        result = []
        with self._lock:
            for symbol, pos in self._positions.items():
                if pos["qty"] <= 0:
                    continue
                price = current_prices.get(symbol, pos["avg_price"])
                if pos["side"] == "BUY":
                    unreal = pos["qty"] * (price - pos["avg_price"])
                else:
                    unreal = pos["qty"] * (pos["avg_price"] - price)
                result.append({
                    "symbol":    symbol,
                    "side":      pos["side"],
                    "quantity":  round(pos["qty"], 6),
                    "avg_price": round(pos["avg_price"], 6),
                    "price":     round(price, 6),
                    "pnl":       round(unreal, 2),
                    "market":    pos.get("market", ""),
                })
        return result


class QuantPythonLayer:

    MIN_TICKS_REQUIRED = 30

    def __init__(self):
        logger.info("=== Polyglot Quant System — Python AI Layer Starting ===")

        self.tick_consumer   = TickConsumer(on_tick=self._on_tick)
        self.signal_producer = SignalProducer()
        self.tech_engine     = TechnicalSignalEngine()
        self.ml_model        = MLEnsembleModel()
        self.claude_engine   = ClaudeReasoningEngine()
        self.portfolio       = PaperPortfolio(config.PAPER_INITIAL_CAPITAL_USD)

        # Per-symbol cooldown tracking
        self._last_eval_time: dict[str, float] = defaultdict(float)

        # Latest price per symbol (updated on every tick)
        self._latest_prices: dict[str, float] = {}

        # Rolling price snapshots for ML label generation
        # Each deque entry is a price captured at evaluation time
        self._price_snapshots: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=config.LABEL_LOOKAHEAD_STEPS + 2)
        )

        # Shared state lists — passed by reference to dashboard
        self._pnl_history:    list[dict] = []
        self._recent_signals: list[dict] = []
        self._lock = threading.Lock()

        # Redis for cross-process dashboard state
        self._redis = self._connect_redis()

    def _connect_redis(self):
        try:
            r = redis.Redis(
                host=config.REDIS_HOST, port=config.REDIS_PORT,
                decode_responses=True, socket_timeout=2,
            )
            r.ping()
            logger.info("Redis connected at {}:{}", config.REDIS_HOST, config.REDIS_PORT)
            return r
        except Exception:
            logger.warning("Redis unavailable — dashboard will use in-process state.")
            return None

    # ── Tick ingestion ────────────────────────────────────────────────────────

    def _on_tick(self, tick: dict):
        """Called for every incoming tick. Updates latest price map."""
        symbol = tick.get("symbol", "")
        price  = tick.get("last") or tick.get("bid", 0)
        if symbol and price > 0:
            self._latest_prices[symbol] = price

    # ── Signal evaluation pipeline ────────────────────────────────────────────

    def _evaluate_symbol(self, symbol: str):
        now = time.time()

        # Enforce per-symbol cooldown
        if now - self._last_eval_time[symbol] < config.SIGNAL_COOLDOWN_SEC:
            return
        self._last_eval_time[symbol] = now

        ticks = self.tick_consumer.get_ticks(symbol)
        logger.debug("{}: {} ticks in window", symbol, len(ticks))
        if len(ticks) < self.MIN_TICKS_REQUIRED:
            logger.debug("{}: too few ticks ({}), skipping", symbol, len(ticks))
            return

        current_tick = ticks[-1]
        market = current_tick.get("market", "UNKNOWN")
        price  = current_tick.get("last") or current_tick.get("bid", 0)
        if price <= 0:
            logger.debug("{}: invalid price {}", symbol, price)
            return

        bid = current_tick.get("bid", price)
        ask = current_tick.get("ask", price)
        spread_bps = (ask - bid) / price * 10_000 if price > 0 else 0

        if spread_bps > config.MAX_SPREAD_BPS:
            logger.debug("{}: spread too wide ({:.1f} bps)", symbol, spread_bps)
            return

        # 1. Technical signals
        tech_result = self.tech_engine.compute(symbol, ticks)
        if tech_result is None:
            logger.debug("{}: tech engine returned None", symbol)
            return

        logger.info("{}: tech dir={} conf={:.2f} score={}", symbol,
                    tech_result.direction, tech_result.confidence,
                    tech_result.signals.get("score", "?"))

        # 2. ML online-learning
        self._tick_ml_labels(symbol, price)

        # 3. ML ensemble prediction
        prices_list = [t.get("last", t.get("bid", 0)) for t in ticks]
        ml_pred = self.ml_model.predict(tech_result.signals, prices_list)

        # Skip neutral technical signals
        if tech_result.direction == 0:
            logger.debug("{}: tech direction neutral, skipping", symbol)
            return

        # 4. Claude AI final decision
        claude_decision = self.claude_engine.reason(
            symbol               = symbol,
            market               = market,
            technical_signals    = tech_result.signals,
            technical_direction  = tech_result.direction,
            technical_confidence = tech_result.confidence,
            ml_prediction        = ml_pred.direction,
            ml_confidence        = ml_pred.confidence,
            current_price        = price,
            spread_bps           = spread_bps,
        )

        decision   = claude_decision.get("decision", "HOLD")
        confidence = claude_decision.get("confidence", 0.0)
        reasoning  = claude_decision.get("reasoning", "")
        size_pct   = claude_decision.get("suggested_size_pct", 0.5)
        risk_flags = claude_decision.get("risk_flags", [])

        if decision not in ("BUY", "SELL") or confidence < config.MIN_CONFIDENCE:
            return

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

        if not published:
            return

        # Paper trading fill
        self.portfolio.fill(symbol, decision, quantity, price, market)

        # Compute trade levels from ATR
        atr_pct = tech_result.signals.get("atr_pct", 1.0)
        levels  = self._compute_trade_levels(price, decision, atr_pct, market)

        # Append level summary to reasoning
        direction_word = "LONG" if decision == "BUY" else "SHORT"
        level_summary = (
            f"Trade setup → {direction_word} {symbol} | "
            f"Entry: {levels['entry']} | "
            f"Stop Loss: {levels['stop_loss']} (-{levels['sl_pct']}%) | "
            f"Take Profit: {levels['take_profit']} (+{levels['tp_pct']}%) | "
            f"Leverage: {levels['leverage']}x"
        )
        full_reasoning = f"{reasoning} {level_summary}"

        # Record signal for dashboard
        record = {
            "timestamp":   time.strftime("%H:%M:%S"),
            "symbol":      symbol,
            "market":      market,
            "side":        decision,
            "quantity":    round(quantity, 6),
            "price":       round(price, 6),
            "confidence":  round(confidence, 3),
            "strategy":    "POLYGLOT_QUANT_V1",
            "reasoning":   full_reasoning,
            "risk_flags":  ", ".join(risk_flags) if risk_flags else "",
            "rsi":         round(tech_result.signals.get("rsi", 0), 1),
            "macd_hist":   round(tech_result.signals.get("macd_hist", 0), 5),
            "ml_dir":      round(ml_pred.direction, 3),
            **levels,
        }
        with self._lock:
            self._recent_signals.append(record)
            if len(self._recent_signals) > 200:
                self._recent_signals = self._recent_signals[-200:]

        self._write_signal_to_redis(record)

    # ── ML label generation ───────────────────────────────────────────────────

    def _tick_ml_labels(self, symbol: str, current_price: float):
        """
        Snapshot current price each evaluation cycle.
        Once LABEL_LOOKAHEAD_STEPS snapshots exist, compute the forward
        return from the oldest snapshot to now and pass it as a label.
        """
        snap = self._price_snapshots[symbol]
        snap.append(current_price)

        if len(snap) >= config.LABEL_LOOKAHEAD_STEPS:
            past_price = snap[0]
            if past_price > 0:
                forward_return = (current_price - past_price) / past_price
                self.ml_model.update_labels([forward_return])

    # ── Trade level computation ───────────────────────────────────────────────

    def _compute_trade_levels(
        self, price: float, side: str, atr_pct: float, market: str
    ) -> dict:
        """Compute entry, stop loss, take profit, and leverage from ATR."""
        atr_abs = price * (atr_pct / 100) if atr_pct else price * 0.01

        if side == "BUY":
            stop_loss   = price - 1.5 * atr_abs
            take_profit = price + 3.0 * atr_abs
        else:  # SELL / short
            stop_loss   = price + 1.5 * atr_abs
            take_profit = price - 3.0 * atr_abs

        sl_pct = abs(price - stop_loss) / price * 100
        tp_pct = abs(take_profit - price) / price * 100

        if atr_pct > 3.0:
            leverage = 2
        elif atr_pct > 1.5:
            leverage = 3
        elif atr_pct > 0.8:
            leverage = 5
        elif atr_pct > 0.3:
            leverage = 7
        else:
            leverage = 10

        if market != "CRYPTO":
            leverage = min(leverage, 5)

        return {
            "entry":       round(price, 6),
            "stop_loss":   round(stop_loss, 6),
            "take_profit": round(take_profit, 6),
            "sl_pct":      round(sl_pct, 2),
            "tp_pct":      round(tp_pct, 2),
            "leverage":    leverage,
        }

    # ── Position sizing ───────────────────────────────────────────────────────

    def _size_position(self, price: float, size_pct: float, market: str) -> float:
        max_usd  = config.MAX_ORDER_USD * max(0.1, size_pct)
        quantity = max_usd / price if price > 0 else 0
        if market == "CRYPTO":
            return round(quantity, 6)
        elif market == "FOREX":
            return round(quantity * 1_000) * 1_000   # standard lot sizing
        else:
            return max(1, round(quantity))

    # ── P&L mark-to-market ────────────────────────────────────────────────────

    def _update_pnl(self):
        pnl = self.portfolio.net_pnl(self._latest_prices)
        record = {"time": time.strftime("%H:%M:%S"), "pnl": pnl}

        with self._lock:
            self._pnl_history.append(record)
            if len(self._pnl_history) > 500:
                self._pnl_history = self._pnl_history[-500:]

        if not self._redis:
            return
        try:
            self._redis.rpush("quant:pnl_history", json.dumps(record))
            self._redis.ltrim("quant:pnl_history", -500, -1)

            positions = self.portfolio.positions_snapshot(self._latest_prices)
            self._redis.delete("quant:positions")
            for pos in positions:
                self._redis.hset("quant:positions", pos["symbol"], json.dumps(pos))
        except Exception as e:
            logger.debug("Redis P&L sync error: {}", e)

    # ── Redis helpers ─────────────────────────────────────────────────────────

    def _write_signal_to_redis(self, record: dict):
        if not self._redis:
            return
        try:
            self._redis.lpush("quant:signals", json.dumps(record))
            self._redis.ltrim("quant:signals", 0, 199)
        except Exception as e:
            logger.debug("Redis signal write error: {}", e)

    # ── Main loop ─────────────────────────────────────────────────────────────

    def run(self):
        self.tick_consumer.start()
        logger.info("Waiting for market data...")

        dashboard_thread = threading.Thread(
            target=start_dashboard,
            kwargs={
                "pnl_source":     self._pnl_history,
                "signals_source": self._recent_signals,
                "portfolio":      self.portfolio,
                "prices_source":  self._latest_prices,
                "state_lock":     self._lock,
            },
            daemon=True,
        )
        dashboard_thread.start()

        pnl_interval  = 5    # seconds between P&L snapshots
        last_pnl_time = 0.0

        try:
            while True:
                symbols = self.tick_consumer.get_symbols()
                for symbol in symbols:
                    try:
                        self._evaluate_symbol(symbol)
                    except Exception as e:
                        logger.error("Error evaluating {}: {}", symbol, e)

                now = time.time()
                if now - last_pnl_time >= pnl_interval:
                    self._update_pnl()
                    last_pnl_time = now

                time.sleep(1)

        except KeyboardInterrupt:
            logger.info("Shutting down Python AI layer...")
        finally:
            self.tick_consumer.stop()
            self.signal_producer.flush()
            logger.info("Python AI layer stopped.")


if __name__ == "__main__":
    QuantPythonLayer().run()
