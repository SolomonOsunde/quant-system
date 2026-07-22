"""
Quant System — Python AI Layer
================================
Consumes ticks from Java via Kafka, runs a multi-factor signal ensemble,
applies regime-gated weighting, sizes positions via Kelly criterion, uses
Claude AI for final trade reasoning, and publishes signals to Java for
execution via Alpaca paper trading.

Signal pipeline per symbol:
  1. Technical indicators (RSI, MACD, Bollinger Bands, EMA, ATR)
  2. Order Flow Imbalance — Lee-Ready classification, VPIN
  3. Time-series + cross-sectional + idiosyncratic momentum
  4. Market regime (Hurst exponent + HMM → strategy weights)
  5. Microstructure features (realized vol, Amihud, Roll spread)
  6. ML ensemble (XGBoost + LightGBM on microstructure features)
  7. Regime-weighted signal ensemble → composite direction + confidence
  8. Kelly criterion + volatility targeting → position size
  9. Claude AI final reasoning and risk gate
 10. Publish to quant.signals Kafka topic

Stat arb pipeline (runs every 60 s):
  - Kalman filter hedge ratios for 5 crypto pairs
  - ADF cointegration test (every 5000 ticks)
  - Z-score entry/exit on each pair's spread
  - Long-only legs: buy underperformer, close outperformer if held
"""

import json
import time
import threading
import numpy as np
from collections import defaultdict, deque
from loguru import logger
import redis

import config
from kafka.tick_consumer import TickConsumer
from kafka.signal_producer import SignalProducer
from kafka.execution_consumer import ExecutionConsumer
from kafka.position_consumer import PositionConsumer
from signals.technical import TechnicalSignalEngine
from signals.order_flow import OrderFlowAnalyzer
from signals.momentum import MomentumEngine
from signals.stat_arb import StatArbEngine
from signals.regime import RegimeDetector, RegimeState
from signals.position_sizing import KellyPositionSizer
from signals.universe import UniverseManager
from signals.slippage import slipped_fill
from signals.calibration import slip_calibrator
from signals.ensemble import (
    build_ensemble,
    compute_trade_levels,
    MIN_HOLD_MINUTES,
    POSITION_MAX_AGE_HOURS,
)
from features.microstructure import realized_vol, amihud_ratio, compute_all as ms_features
from ml.ensemble import MLEnsembleModel
from ai.claude_engine import ClaudeReasoningEngine
from dashboard.app import start_dashboard
from ops.alerts import alerts


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

    @property
    def open_count(self) -> int:
        with self._lock:
            return sum(1 for p in self._positions.values() if p.get("qty", 0) > 0)

    def fill(self, symbol: str, side: str, quantity: float, price: float, market: str,
             stop_loss: float = None, take_profit: float = None,
             live_broker: bool = False):
        cost = quantity * price
        with self._lock:
            pos = self._positions.get(symbol, {
                "qty": 0.0, "avg_price": 0.0, "side": "FLAT", "market": market
            })

            if side == "BUY":
                if pos["side"] == "SELL" and pos["qty"] > 0:
                    # Cover short then optionally open long with remainder
                    close_qty = min(quantity, pos["qty"])
                    self._realized_pnl += close_qty * (pos["avg_price"] - price)
                    self.cash -= close_qty * price
                    leftover_short = pos["qty"] - close_qty
                    remainder = quantity - close_qty
                    if leftover_short > 1e-12:
                        pos = {
                            "qty": leftover_short, "avg_price": pos["avg_price"],
                            "side": "SELL", "market": market,
                            "entry_time": pos.get("entry_time", time.time()),
                            "stop_loss": pos.get("stop_loss"),
                            "take_profit": pos.get("take_profit"),
                            "live_broker": pos.get("live_broker", live_broker),
                        }
                    elif remainder > 1e-12:
                        pos = {
                            "qty": remainder, "avg_price": price,
                            "side": "BUY", "market": market,
                        }
                        self.cash -= remainder * price
                    else:
                        pos = {"qty": 0.0, "avg_price": 0.0, "side": "FLAT", "market": market}
                else:
                    total_qty = pos["qty"] + quantity
                    pos["avg_price"] = (
                        (pos["qty"] * pos.get("avg_price", price) + cost) / total_qty
                    )
                    pos["qty"] = total_qty
                    pos["side"] = "BUY"
                    self.cash -= cost

            elif side == "SELL":
                if pos["side"] == "BUY" and pos["qty"] > 0:
                    # Close long then optionally open short with remainder
                    close_qty = min(quantity, pos["qty"])
                    self._realized_pnl += close_qty * (price - pos["avg_price"])
                    self.cash += close_qty * price
                    leftover_long = pos["qty"] - close_qty
                    remainder = quantity - close_qty
                    if leftover_long > 1e-12:
                        pos = {
                            "qty": leftover_long, "avg_price": pos["avg_price"],
                            "side": "BUY", "market": market,
                            "entry_time": pos.get("entry_time", time.time()),
                            "stop_loss": pos.get("stop_loss"),
                            "take_profit": pos.get("take_profit"),
                            "live_broker": pos.get("live_broker", live_broker),
                        }
                    elif remainder > 1e-12:
                        # Crypto long-only: do not open short remainder
                        if market == "CRYPTO":
                            pos = {"qty": 0.0, "avg_price": 0.0, "side": "FLAT", "market": market}
                        else:
                            pos = {
                                "qty": remainder, "avg_price": price,
                                "side": "SELL", "market": market,
                            }
                            self.cash += remainder * price
                    else:
                        pos = {"qty": 0.0, "avg_price": 0.0, "side": "FLAT", "market": market}
                else:
                    if market == "CRYPTO":
                        # Long-only: ignore naked short opens
                        return
                    total_qty = pos["qty"] + quantity
                    pos["avg_price"] = (
                        (pos["qty"] * pos.get("avg_price", price) + cost) / total_qty
                    )
                    pos["qty"] = total_qty
                    pos["side"] = "SELL"
                    self.cash += cost

            if pos.get("qty", 0) > 0:
                pos["market"] = market
                if pos.get("entry_time") is None:
                    pos["entry_time"] = time.time()
                if stop_loss is not None:
                    pos["stop_loss"] = stop_loss
                if take_profit is not None:
                    pos["take_profit"] = take_profit
                if live_broker:
                    pos["live_broker"] = True
                elif "live_broker" not in pos:
                    pos["live_broker"] = False
                self._positions[symbol] = pos
            else:
                self._positions.pop(symbol, None)

    def close_position(self, symbol: str, price: float, reason: str = "MANUAL") -> dict | None:
        """Close an open position at price. Returns close record or None if no position."""
        with self._lock:
            pos = self._positions.get(symbol)
            if not pos or pos.get("qty", 0) <= 0:
                return None
            qty  = pos["qty"]
            avg  = pos["avg_price"]
            side = pos["side"]
            live_broker = bool(pos.get("live_broker", False))
            market = pos.get("market", "")
            if side == "BUY":
                pnl = qty * (price - avg)
                self.cash += qty * price
            else:
                pnl = qty * (avg - price)
                self.cash -= qty * price
            self._realized_pnl += pnl
            del self._positions[symbol]
            return {
                "symbol":      symbol,
                "qty":         qty,
                "avg_price":   avg,
                "close_price": price,
                "pnl":         pnl,
                "reason":      reason,
                "side":        side,
                "live_broker": live_broker,
                "market":      market,
            }

    def check_exits(self, current_prices: dict,
                    max_age_hours: float = 2.0,
                    min_hold_minutes: float = 30.0) -> list[dict]:
        """
        Return list of positions that have hit SL, TP, or time-stop.

        min_hold_minutes: SL/TP not checked until this many minutes have
            elapsed. Gives the trade room to develop — backtest shows
            30-minute hold has positive expectancy vs 5-minute which loses.
        max_age_hours: force-close if position has been open this long.
        """
        triggered = []
        now = time.time()
        min_hold_hours = min_hold_minutes / 60.0
        with self._lock:
            for symbol, pos in self._positions.items():
                if pos.get("qty", 0) <= 0:
                    continue
                price = current_prices.get(symbol)
                if not price:
                    continue
                sl         = pos.get("stop_loss")
                tp         = pos.get("take_profit")
                entry_time = pos.get("entry_time", now)
                age_hours  = (now - entry_time) / 3600.0

                if age_hours >= max_age_hours:
                    triggered.append({"symbol": symbol, "price": price, "reason": "TIME_STOP"})
                    continue

                # Don't check SL/TP during minimum hold window
                if age_hours < min_hold_hours:
                    continue

                if sl:
                    if pos["side"] == "BUY"  and price <= sl:
                        triggered.append({"symbol": symbol, "price": price, "reason": "STOP_LOSS"})
                        continue
                    if pos["side"] == "SELL" and price >= sl:
                        triggered.append({"symbol": symbol, "price": price, "reason": "STOP_LOSS"})
                        continue

                if tp:
                    if pos["side"] == "BUY"  and price >= tp:
                        triggered.append({"symbol": symbol, "price": price, "reason": "TAKE_PROFIT"})
                        continue
                    if pos["side"] == "SELL" and price <= tp:
                        triggered.append({"symbol": symbol, "price": price, "reason": "TAKE_PROFIT"})
                        continue
        return triggered

    def net_pnl(self, current_prices: dict) -> float:
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

    def sync_live_from_broker(self, positions: list[dict]):
        """
        Align live_broker crypto positions with Alpaca snapshot.
        Simulation-only positions are left untouched.
        """
        broker = {
            p["symbol"]: p for p in positions
            if p.get("symbol") and abs(float(p.get("qty", 0) or 0)) > 1e-12
        }
        with self._lock:
            # Remove live crypto positions missing from broker
            for sym in list(self._positions.keys()):
                pos = self._positions[sym]
                if not pos.get("live_broker"):
                    continue
                if "/" not in sym:
                    continue
                if sym not in broker:
                    del self._positions[sym]

            for sym, bp in broker.items():
                qty = abs(float(bp.get("qty", 0) or 0))
                avg = float(bp.get("avgPrice") or bp.get("avg_price") or 0)
                side = bp.get("side", "BUY")
                if side == "SELL" or float(bp.get("qty", 0) or 0) < 0:
                    # Crypto long-only — ignore short broker positions
                    continue
                existing = self._positions.get(sym)
                if existing and not existing.get("live_broker"):
                    continue  # don't overwrite sim paper
                if existing and abs(existing.get("qty", 0) - qty) < 1e-8:
                    existing["avg_price"] = avg if avg > 0 else existing.get("avg_price", avg)
                    existing["live_broker"] = True
                    continue
                self._positions[sym] = {
                    "qty": qty,
                    "avg_price": avg,
                    "side": "BUY",
                    "market": "CRYPTO",
                    "live_broker": True,
                    "entry_time": (existing or {}).get("entry_time", time.time()),
                    "stop_loss": (existing or {}).get("stop_loss"),
                    "take_profit": (existing or {}).get("take_profit"),
                }

    def positions_snapshot(self, current_prices: dict) -> list[dict]:
        result = []
        with self._lock:
            for symbol, pos in self._positions.items():
                if pos["qty"] <= 0:
                    continue
                price = current_prices.get(symbol, pos["avg_price"])
                unreal = (
                    pos["qty"] * (price - pos["avg_price"])
                    if pos["side"] == "BUY"
                    else pos["qty"] * (pos["avg_price"] - price)
                )
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

    MIN_TICKS_REQUIRED         = 30
    CROSS_SECTIONAL_INTERVAL   = 30.0   # seconds between CS momentum refresh
    STAT_ARB_INTERVAL          = 60.0   # seconds between stat arb evaluation
    REGIME_LOG_INTERVAL        = 120.0  # seconds between regime summary log
    MONITOR_INTERVAL           = 2.0    # seconds between position monitor checks
    MAX_OPEN_POSITIONS         = 5      # max simultaneous open positions
    MAX_DAILY_LOSS_PCT         = 0.05   # halt new entries if daily loss > 5% of capital
    POSITION_MAX_AGE_HOURS     = POSITION_MAX_AGE_HOURS  # shared with backtest
    MIN_HOLD_MINUTES           = MIN_HOLD_MINUTES        # shared with backtest

    def __init__(self):
        logger.info("=== Polyglot Quant System — Python AI Layer Starting ===")
        logger.info("Signal pipeline: Technical + OFI + Momentum + Regime + ML + Kelly + Claude")
        logger.info(
            "Live mode: ALLOW_SIMULATION={} PAPER_TRADING={} REQUIRE_FILL_ACK={}",
            getattr(config, "ALLOW_SIMULATION", False),
            config.PAPER_TRADING,
            config.REQUIRE_FILL_ACK,
        )
        if not getattr(config, "ALLOW_SIMULATION", False):
            logger.info("Simulation ticks rejected — broker feeds only")

        self.tick_consumer   = TickConsumer(on_tick=self._on_tick)
        self.signal_producer = SignalProducer()
        self.exec_consumer   = ExecutionConsumer(on_execution=self._on_execution)
        self.pos_consumer    = PositionConsumer(on_positions=self._on_positions)
        self.tech_engine     = TechnicalSignalEngine()
        self.ml_model        = MLEnsembleModel()
        self.claude_engine   = ClaudeReasoningEngine()
        self.portfolio       = PaperPortfolio(config.PAPER_INITIAL_CAPITAL_USD)
        self._day_utc        = None
        self._day_start_equity = config.PAPER_INITIAL_CAPITAL_USD

        # Pending broker acks: symbol → pending fill/close payload
        self._pending_entries: dict[str, dict] = {}
        self._pending_exits:   dict[str, dict] = {}

        # Advanced signal engines
        self._order_flow  = OrderFlowAnalyzer()
        self._momentum    = MomentumEngine()
        self._stat_arb    = StatArbEngine()
        self._regime:     dict[str, RegimeDetector] = defaultdict(RegimeDetector)
        self._sizer       = KellyPositionSizer(
            capital_usd   = config.PAPER_INITIAL_CAPITAL_USD,
            max_order_usd = config.MAX_ORDER_USD,
        )
        self._universe_mgr = UniverseManager()

        # Cross-sectional momentum cache (updated every CROSS_SECTIONAL_INTERVAL)
        self._cs_momentum:   dict[str, float] = {}
        self._idio_momentum: dict[str, float] = {}

        self._last_eval_time:   dict[str, float] = defaultdict(float)
        self._latest_prices:    dict[str, float] = {}
        self._price_snapshots:  dict[str, deque] = defaultdict(
            lambda: deque(maxlen=config.LABEL_LOOKAHEAD_STEPS + 2)
        )

        self._pnl_history:    list[dict] = []
        self._recent_signals: list[dict] = []
        self._symbol_scores:  dict[str, dict] = {}   # live per-symbol evaluation state for dashboard
        self._lock = threading.Lock()
        self._last_monitor_ts = 0.0

        self._last_cs_update   = 0.0
        self._last_stat_arb_ts = 0.0
        self._last_regime_log  = 0.0

        # Live capital / ops state from broker snapshots
        self._broker_equity: float | None = None
        self._broker_kill = False
        self._stat_arb_legs: dict[str, str] = {}  # pair → buy leg currently held for SA

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

    # ── Tick ingestion ─────────────────────────────────────────────────────────

    def _on_tick(self, tick: dict):
        source = (tick.get("source") or "").upper()
        if not getattr(config, "ALLOW_SIMULATION", False) and source in ("SIMULATION", "SIM"):
            return
        symbol = tick.get("symbol", "")
        price  = tick.get("last") or tick.get("bid", 0)
        if symbol and price > 0:
            self._latest_prices[symbol] = price
        self._order_flow.update(tick)

    def _on_execution(self, fill: dict):
        """Apply broker-confirmed fills to the local paper book."""
        symbol = fill.get("symbol", "")
        side = fill.get("side", "").upper()
        qty = float(fill.get("qty", 0) or 0)
        price = float(fill.get("price", 0) or 0)
        status = (fill.get("status") or "filled").lower()
        is_exit = bool(fill.get("isExit", False))
        signal_price = float(fill.get("signalPrice") or 0)

        # Non-fill outcomes: clear pending, alert, do not book
        ok_status = status in ("filled", "partial")
        if not ok_status or qty <= 0 or price <= 0:
            with self._lock:
                self._pending_entries.pop(symbol, None)
                self._pending_exits.pop(symbol, None)
            if symbol:
                alerts.emit(
                    "ERROR", "BROKER_NO_FILL",
                    f"Broker outcome {status} for {symbol}",
                    status=status, side=side,
                )
            return

        if not symbol:
            return

        # Calibrate model vs actual fill
        if signal_price > 0:
            slip_calibrator.record(symbol, side, signal_price, price)

        with self._lock:
            pending_exit = self._pending_exits.pop(symbol, None)
            pending_entry = self._pending_entries.pop(symbol, None)

        if is_exit or pending_exit:
            meta = pending_exit or {}
            result = self.portfolio.close_position(symbol, price, meta.get("reason", "BROKER_FILL"))
            if result and hasattr(self._sizer, "record_outcome"):
                self._sizer.record_outcome(result["pnl"], 1.0)
            logger.info(
                "ACK EXIT {} {} qty={:.6g} @ {:.4f} orderId={} status={}",
                side, symbol, qty, price, fill.get("orderId"), status,
            )
            return

        if pending_entry:
            self.portfolio.fill(
                symbol, side, qty, price,
                pending_entry.get("market", "CRYPTO"),
                stop_loss=pending_entry.get("stop_loss"),
                take_profit=pending_entry.get("take_profit"),
                live_broker=True,
            )
            logger.info(
                "ACK ENTRY {} {} qty={:.6g} @ {:.4f} orderId={} status={}",
                side, symbol, qty, price, fill.get("orderId"), status,
            )
            return

        # Unexpected fill (e.g. after restart hydrate mismatch) — book it
        market = "FOREX" if self._is_forex(symbol) else ("CRYPTO" if "/" in symbol else "EQUITY")
        self.portfolio.fill(symbol, side, qty, price, market, live_broker=True)
        logger.info("ACK orphan fill {} {} qty={:.6g} @ {:.4f}", side, symbol, qty, price)

    _FOREX_CCY = frozenset({"EUR", "GBP", "USD", "JPY", "AUD", "CAD", "CHF", "NZD"})

    @classmethod
    def _is_forex(cls, symbol: str) -> bool:
        if not symbol:
            return False
        parts = symbol.upper().replace("_", "/").split("/")
        if len(parts) != 2:
            return False
        return parts[0] in cls._FOREX_CCY and parts[1] in cls._FOREX_CCY

    def _on_positions(self, snapshot: dict):
        """Broker position snapshot is source of truth for live books."""
        positions = snapshot.get("positions") or []
        try:
            equity = float(snapshot.get("equity") or 0)
            if equity > 0:
                prev = self._broker_equity
                self._broker_equity = equity
                self._sizer.set_capital(equity)
                if prev is not None and abs(equity - prev) / max(prev, 1) > 0.15:
                    alerts.emit("WARN", "EQUITY_JUMP", "Broker equity moved >15%",
                                prev=prev, equity=equity)

            kill = bool(snapshot.get("killSwitch", False))
            if kill and not self._broker_kill:
                alerts.emit("CRITICAL", "KILL_SWITCH",
                            "Java risk kill switch active — halting new entries",
                            daily_pnl=snapshot.get("dailyPnl"))
            self._broker_kill = kill

            # Desync: live local qty vs broker (crypto + forex slash symbols)
            broker_map = {
                p["symbol"]: abs(float(p.get("qty", 0) or 0))
                for p in positions if p.get("symbol")
            }
            with self.portfolio._lock:
                for sym, pos in self.portfolio._positions.items():
                    if not pos.get("live_broker"):
                        continue
                    local_q = float(pos.get("qty", 0) or 0)
                    bro_q = broker_map.get(sym, 0.0)
                    if abs(local_q - bro_q) > max(1e-6, 0.01 * max(local_q, bro_q, 1e-9)):
                        alerts.emit(
                            "WARN", "BOOK_DESYNC",
                            f"Local vs broker qty mismatch for {sym}",
                            local=local_q, broker=bro_q,
                        )

            self.portfolio.sync_live_from_broker(positions)
            logger.debug("Synced {} broker position(s) equity={}", len(positions), equity or "n/a")
        except Exception as e:
            alerts.emit("ERROR", "POSITION_SYNC", f"Position sync error: {e}")
            logger.error("Position sync error: {}", e)

    # ── Cross-sectional refresh ────────────────────────────────────────────────

    def _refresh_cross_sectional(self):
        all_ticks = {
            sym: self.tick_consumer.get_ticks(sym)
            for sym in self.tick_consumer.get_symbols()
        }
        all_prices = {
            sym: [t.get("last") or t.get("bid", 0) for t in ticks if (t.get("last") or t.get("bid", 0)) > 0]
            for sym, ticks in all_ticks.items()
        }

        # Refresh universe
        if self._universe_mgr.needs_refresh():
            self._universe_mgr.refresh(all_ticks)
            self._universe_mgr.log_universe_table()

        # Cross-sectional + idiosyncratic momentum
        for sym in all_prices:
            self._cs_momentum[sym]   = self._momentum.compute_cross_sectional(sym, all_prices)
            self._idio_momentum[sym] = self._momentum.compute_idiosyncratic(sym, all_prices)

    # ── Signal evaluation pipeline ─────────────────────────────────────────────

    def _evaluate_symbol(self, symbol: str):
        now = time.time()
        if now - self._last_eval_time[symbol] < config.SIGNAL_COOLDOWN_SEC:
            return

        # Universe gate — skip symbols the volatility ranker hasn't selected
        universe = self._universe_mgr.active_universe
        if universe and symbol not in universe:
            return

        self._last_eval_time[symbol] = now

        ticks   = self.tick_consumer.get_ticks(symbol)
        n_ticks = len(ticks)

        # Always update scanner so dashboard reflects live state at every gate
        self._symbol_scores[symbol] = {
            **self._symbol_scores.get(symbol, {}),
            "symbol":  symbol,
            "ticks":   n_ticks,
            "price":   self._latest_prices.get(symbol, 0),
            "updated": time.strftime("%H:%M:%S"),
            "status":  "WARMING",
        }
        logger.debug("{}: {} ticks in window", symbol, n_ticks)
        if n_ticks < self.MIN_TICKS_REQUIRED:
            return

        current_tick  = ticks[-1]
        market        = current_tick.get("market", "UNKNOWN")
        price         = current_tick.get("last") or current_tick.get("bid", 0)
        tick_source   = (current_tick.get("source") or "").upper()
        if not getattr(config, "ALLOW_SIMULATION", False) and tick_source in ("SIMULATION", "SIM"):
            return
        is_simulation = tick_source in ("SIMULATION", "SIM")
        if price <= 0:
            return

        bid        = current_tick.get("bid", price)
        ask        = current_tick.get("ask", price)
        spread_bps = (ask - bid) / price * 10_000 if price > 0 else 0

        self._symbol_scores[symbol].update({"price": price, "spread_bps": round(spread_bps, 1)})
        if spread_bps > config.MAX_SPREAD_BPS:
            self._symbol_scores[symbol]["status"] = "SPREAD_WIDE"
            return

        prices_list = [t.get("last") or t.get("bid", 0) for t in ticks]
        prices_list = [p for p in prices_list if p > 0]

        # ── 1. Technical indicators ───────────────────────────────────────────
        tech_result = self.tech_engine.compute(symbol, ticks)
        if tech_result is None:
            return

        # ── 2. Microstructure features ────────────────────────────────────────
        ms = ms_features(ticks, prices_list)
        rv = ms["realized_vol"]

        # ── 3. Order flow imbalance ───────────────────────────────────────────
        ofi = self._order_flow.compute(symbol)

        # ── 4. Time-series + cross-sectional momentum ─────────────────────────
        mom_result = self._momentum.compute_ts_momentum(prices_list)
        cs_mom     = self._cs_momentum.get(symbol, 0.0)
        idio_mom   = self._idio_momentum.get(symbol, 0.0)

        # ── 5. Regime detection ───────────────────────────────────────────────
        regime = self._regime[symbol].compute(prices_list)

        if regime and regime.state == RegimeState.CRISIS:
            self._symbol_scores[symbol].update({"status": "CRISIS", "regime": "CRISIS"})
            logger.info("{}: Crisis regime — no trading", symbol)
            return

        pos_scale = regime.position_scale if regime else 1.0

        # ── 6. ML prediction ──────────────────────────────────────────────────
        self._tick_ml_labels(symbol, price)

        enriched_signals = {
            **tech_result.signals,
            "realized_vol":  rv,
            "amihud":        ms["amihud"],
            "autocorr_lag1": ms["autocorr_lag1"],
            "ofi_short":     ofi.ofi_short if ofi else 0.0,
            "cs_momentum":   cs_mom,
        }
        ml_pred = self.ml_model.predict(enriched_signals, prices_list, symbol=symbol)

        # ── 7. Regime-weighted ensemble (shared with backtest) ────────────────
        ml_trained = getattr(ml_pred, "confidence", 0) > 0
        ensemble = build_ensemble(
            tech_result, ml_pred, ofi, mom_result, cs_mom, idio_mom, regime,
            enable_ml=ml_trained,
            enable_ofi=ofi is not None,
        )

        direction = ensemble["direction"]
        composite = ensemble["composite"]

        regime_name = regime.state.name if regime else "—"
        self._symbol_scores[symbol].update({
            "composite": round(composite, 4),
            "rsi":       round(tech_result.signals.get("rsi", 50), 1),
            "regime":    regime_name,
        })

        if direction == 0:
            self._symbol_scores[symbol].update({"direction": "—", "confidence": 0.0, "status": "NEUTRAL"})
            logger.debug("{}: ensemble neutral (composite={:.3f})", symbol, composite)
            return

        # ── 8. Claude AI final reasoning ──────────────────────────────────────
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
            ofi                  = ofi,
            mom_result           = mom_result,
            cs_momentum          = cs_mom,
            idio_momentum        = idio_mom,
            regime               = regime,
            composite_score      = composite,
            ms_features          = ms,
        )

        decision   = claude_decision.get("decision", "HOLD")
        confidence = claude_decision.get("confidence", 0.0)
        reasoning  = claude_decision.get("reasoning", "")
        risk_flags = list(claude_decision.get("risk_flags", []) or [])

        # Claude may HOLD or confirm — never reverse the ensemble direction
        ens_side = "BUY" if direction > 0 else "SELL"
        if decision in ("BUY", "SELL") and decision != ens_side:
            risk_flags.append(f"Claude reversed ensemble ({decision} vs {ens_side}) — blocked")
            decision = "HOLD"

        # Adverse selection gate
        if ofi and getattr(ofi, "informed_flag", False):
            risk_flags.append("High VPIN — adverse selection risk")
            if confidence < config.MIN_CONFIDENCE + 0.1:
                decision = "HOLD"

        # Crypto: long-only — SELL signals close longs; never open shorts
        with self.portfolio._lock:
            pos = self.portfolio._positions.get(symbol, {})
            has_long = pos.get("qty", 0) > 0 and pos.get("side") == "BUY"

        if market == "CRYPTO" and decision == "SELL":
            if has_long:
                # Treat as exit signal — close full position via exit path
                self._close_position(symbol, price, "ENSEMBLE_REVERSE")
                return
            decision = "HOLD"
            risk_flags.append("Crypto long-only — SELL skipped (no long)")

        self._symbol_scores[symbol].update({
            "direction":  decision,
            "confidence": round(confidence, 3),
            "status":     "CANDIDATE",
            "reasoning":  reasoning[:100],
            "risk_flags": ", ".join(risk_flags) if risk_flags else "",
        })

        # Log every directional candidate so dashboard can show sub-threshold signals
        logger.info(
            "CANDIDATE {}: {} conf={:.2f} composite={:.3f} flags={}",
            symbol, decision, confidence, composite, risk_flags,
        )

        if decision not in ("BUY", "SELL") or confidence < config.MIN_CONFIDENCE:
            self._record_candidate(symbol, market, decision, confidence, price,
                                   spread_bps, composite, risk_flags, reasoning,
                                   tech_result, is_simulation)
            return

        # ── 9. Kelly position sizing ──────────────────────────────────────────
        deployed = 0.0
        with self.portfolio._lock:
            p = self.portfolio._positions.get(symbol, {})
            if p.get("qty", 0) > 0:
                deployed = abs(p["qty"] * p.get("avg_price", price))

        sizing = self._sizer.compute(
            confidence           = confidence,
            price                = price,
            realized_vol         = max(rv, 0.01),
            spread_bps           = spread_bps,
            capital_deployed_usd = deployed,
            adv_usd              = self._estimate_adv_usd(symbol, price),
        )

        final_usd = sizing.recommended_usd * pos_scale
        quantity  = self._to_quantity(final_usd, price, market)
        if quantity <= 0:
            return

        # ── Risk gates ────────────────────────────────────────────────────────
        if self._broker_kill:
            alerts.emit("WARN", "ENTRY_BLOCKED_KILL", f"Kill switch blocked {symbol}")
            return
        if not self._daily_loss_ok():
            return
        if not self._can_open_position(symbol, decision, market):
            logger.info("Position gate blocked {} {} — already {} or cap reached", decision, symbol, decision)
            return

        # ── 10. Compute trade levels before fill so SL/TP are stored on position ─
        atr_pct = tech_result.signals.get("atr_pct", 1.0)
        levels  = compute_trade_levels(price, decision, atr_pct, market)

        # Stress slippage for paper/sim fills (live broker uses actual fill price)
        fill_price = price
        if getattr(config, "STRESS_SLIPPAGE", True):
            vpin = float(getattr(ofi, "vpin", 0) or 0) if ofi else 0.0
            vol_ratio = float(tech_result.signals.get("volume_ratio", 1.0) or 1.0)
            sr = slipped_fill(
                price, decision,
                spread_bps=spread_bps,
                atr_pct=float(atr_pct or 0.5),
                order_usd=final_usd,
                adv_usd=self._estimate_adv_usd(symbol, price),
                vpin=vpin,
                volume_ratio=vol_ratio,
                stress_scale=slip_calibrator.stress_scale(),
            )
            fill_price = sr.fill_price
            if sr.stress:
                risk_flags.append(f"Stress slip {sr.slip_bps:.1f}bps")

        # ── 11. Publish signal ────────────────────────────────────────────────
        # PAPER_TRADING=False forces simulation (never hit a real broker path)
        broker_live = (not is_simulation) and bool(config.PAPER_TRADING)
        if not config.PAPER_TRADING and not is_simulation:
            logger.warning("PAPER_TRADING=False — forcing simulation for {}", symbol)
        published = self.signal_producer.publish_signal(
            symbol     = symbol,
            side       = decision,
            quantity   = quantity,
            price      = fill_price if not broker_live else price,
            order_type = "MARKET",
            strategy   = "POLYGLOT_QUANT_V2",
            confidence = confidence,
            spread_bps = spread_bps,
            reasoning  = reasoning,
            simulation = not broker_live,
            entry_price = price,
        )
        if not published:
            alerts.emit("ERROR", "SIGNAL_PUBLISH", f"Failed to publish entry for {symbol}")
            return

        use_ack = broker_live and getattr(config, "REQUIRE_FILL_ACK", True)
        if use_ack:
            with self._lock:
                self._pending_entries[symbol] = {
                    "market": market,
                    "stop_loss": levels["stop_loss"],
                    "take_profit": levels["take_profit"],
                    "side": decision,
                    "qty": quantity,
                    "price": price,
                    "ts": time.time(),
                }
            logger.info("PENDING ENTRY {} {} qty={:.6g} — awaiting broker ack", decision, symbol, quantity)
        else:
            self.portfolio.fill(
                symbol, decision, quantity, fill_price, market,
                stop_loss=levels["stop_loss"], take_profit=levels["take_profit"],
                live_broker=broker_live,
            )
        self._symbol_scores[symbol]["status"] = "SIGNAL"
        direction_word = "LONG" if decision == "BUY" else "SHORT"
        level_summary  = (
            f"Trade setup → {direction_word} {symbol} | "
            f"Entry: {levels['entry']} | "
            f"SL: {levels['stop_loss']} (-{levels['sl_pct']}%) | "
            f"TP: {levels['take_profit']} (+{levels['tp_pct']}%) | "
            f"Leverage: {levels['leverage']}x | "
            f"Kelly: {sizing.kelly_fraction:.3f} ({sizing.method}) | "
            f"Regime: {regime.state.name if regime else 'N/A'}"
        )
        full_reasoning = f"{reasoning} {level_summary}"

        record = {
            "timestamp":    time.strftime("%H:%M:%S"),
            "symbol":       symbol,
            "market":       market,
            "side":         decision,
            "quantity":     round(quantity, 6),
            "price":        round(price, 6),
            "confidence":   round(confidence, 3),
            "spread_bps":   round(spread_bps, 2),
            "reasoning":    full_reasoning,
            "risk_flags":   risk_flags,
            "composite":    round(composite, 4),
            "rsi":          tech_result.signals.get("rsi", 0),
            "entry":        levels["entry"],
            "stop_loss":    levels["stop_loss"],
            "take_profit":  levels["take_profit"],
            "sl_pct":       levels["sl_pct"],
            "tp_pct":       levels["tp_pct"],
            "leverage":     levels["leverage"],
            "simulation":   is_simulation,
        }

        with self._lock:
            self._recent_signals.append(record)
            if len(self._recent_signals) > 200:
                self._recent_signals = self._recent_signals[-200:]

        self._write_redis_signal(record)

        logger.info(
            "SIGNAL {} {} {} qty={:.6g} price={:.4f} conf={:.0%} | {}",
            decision, symbol, market, quantity, price, confidence, reasoning[:80],
        )

    # ── Candidate recorder (sub-threshold directional signals) ────────────────

    def _record_candidate(self, symbol, market, decision, confidence, price,
                           spread_bps, composite, risk_flags, reasoning,
                           tech_result, is_simulation):
        """Store sub-threshold directional signals for dashboard visibility."""
        record = {
            "timestamp":  time.strftime("%H:%M:%S"),
            "symbol":     symbol,
            "market":     market,
            "side":       decision,
            "price":      round(price, 6),
            "confidence": round(confidence, 3),
            "spread_bps": round(spread_bps, 2),
            "composite":  round(composite, 4),
            "reasoning":  reasoning,
            "risk_flags": risk_flags,
            "rsi":        tech_result.signals.get("rsi", 0),
            "candidate":  True,
        }
        if self._redis:
            try:
                self._redis.lpush("quant:candidates", json.dumps(record))
                self._redis.ltrim("quant:candidates", 0, 49)
            except Exception:
                pass

    # ── Stat arb evaluation ────────────────────────────────────────────────────

    def _evaluate_stat_arb_pairs(self):
        """
        Long-only executable stat-arb for crypto (no shorts on Alpaca):
          z > 0 (Y rich) → BUY X (underperformer), CLOSE Y if held
          z < 0 (Y cheap) → BUY Y, CLOSE X if held
          exit/stop → close any open SA buy-leg for the pair
        """
        all_prices = {}
        for sym in self.tick_consumer.get_symbols():
            ticks = self.tick_consumer.get_ticks(sym)
            prices = [t.get("last") or t.get("bid", 0) for t in ticks if (t.get("last") or t.get("bid", 0)) > 0]
            if prices:
                all_prices[sym] = prices

        self._stat_arb.update(all_prices)
        pair_signals = self._stat_arb.get_all_signals(all_prices)

        if self._broker_kill or not self._daily_loss_ok():
            return

        for ps in pair_signals:
            pair = ps.pair
            if ps.signal_type in ("exit", "stop"):
                buy_leg = self._stat_arb_legs.get(pair)
                if buy_leg:
                    px = self._latest_prices.get(buy_leg)
                    if px:
                        self._close_position(buy_leg, px, f"STAT_ARB_{ps.signal_type.upper()}")
                    self._stat_arb_legs.pop(pair, None)
                continue

            if ps.signal_type != "entry" or ps.direction == 0:
                continue

            # z>0 → BUY leg1 (X); z<0 → BUY leg2 (Y). Close the other if long.
            if ps.z_score > 0:
                buy_leg, close_leg = ps.leg1, ps.leg2
            else:
                buy_leg, close_leg = ps.leg2, ps.leg1

            close_px = self._latest_prices.get(close_leg)
            closed_usd = 0.0
            with self.portfolio._lock:
                close_pos = self.portfolio._positions.get(close_leg, {})
                has_close = close_pos.get("qty", 0) > 0 and close_pos.get("side") == "BUY"
                if has_close:
                    closed_usd = abs(close_pos["qty"] * close_pos.get("avg_price", close_px or 0))
            if has_close and close_px:
                self._close_position(close_leg, close_px, "STAT_ARB_ROTATE")

            buy_px = self._latest_prices.get(buy_leg)
            if not buy_px or buy_px <= 0:
                continue
            if not self._can_open_position(buy_leg, "BUY", "CRYPTO"):
                continue

            conf = float(min(0.92, 0.55 + max(abs(ps.z_score) - 2.0, 0.0) * 0.08))
            if conf < config.MIN_CONFIDENCE:
                continue

            ticks = self.tick_consumer.get_ticks(buy_leg)
            tick = ticks[-1] if ticks else {}
            bid = tick.get("bid", buy_px)
            ask = tick.get("ask", buy_px)
            spread_bps = (ask - bid) / buy_px * 10_000 if buy_px > 0 else 5.0
            is_simulation = (tick.get("source") or "").upper() in ("SIMULATION", "SIM")
            if is_simulation and not getattr(config, "ALLOW_SIMULATION", False):
                continue

            regime = self._regime[buy_leg].compute(all_prices.get(buy_leg, []))
            pos_scale = regime.position_scale if regime else 1.0
            rv = max((regime.realized_vol if regime else 0.5), 0.01)

            deployed = 0.0
            with self.portfolio._lock:
                p = self.portfolio._positions.get(buy_leg, {})
                if p.get("qty", 0) > 0:
                    deployed = abs(p["qty"] * p.get("avg_price", buy_px))

            sizing = self._sizer.compute(
                confidence=conf, price=buy_px, realized_vol=rv,
                spread_bps=spread_bps, capital_deployed_usd=deployed,
                adv_usd=self._estimate_adv_usd(buy_leg, buy_px),
            )
            # β-notional: when rotating off the rich leg, size buy ≈ closed_usd / β
            # (spread = log Y − α − β log X → dollar hedge scale β)
            beta = max(abs(ps.hedge_ratio), 0.05)
            kelly_usd = sizing.recommended_usd * pos_scale
            if closed_usd > 0:
                hedge_usd = closed_usd / beta if ps.z_score > 0 else closed_usd * beta
                final_usd = min(kelly_usd, hedge_usd) if kelly_usd > 0 else hedge_usd
            else:
                final_usd = kelly_usd
            quantity = self._to_quantity(final_usd, buy_px, "CRYPTO")
            if quantity <= 0:
                continue

            atr_pct = 1.0
            if ticks and len(ticks) >= self.MIN_TICKS_REQUIRED:
                tech = self.tech_engine.compute(buy_leg, ticks)
                if tech:
                    atr_pct = tech.signals.get("atr_pct", 1.0)
            levels = compute_trade_levels(buy_px, "BUY", atr_pct, "CRYPTO")

            fill_price = buy_px
            if getattr(config, "STRESS_SLIPPAGE", True):
                ofi = self._order_flow.compute(buy_leg)
                sr = slipped_fill(
                    buy_px, "BUY",
                    spread_bps=spread_bps,
                    atr_pct=float(atr_pct or 0.5),
                    order_usd=final_usd,
                    adv_usd=self._estimate_adv_usd(buy_leg, buy_px),
                    vpin=float(getattr(ofi, "vpin", 0) or 0) if ofi else 0.0,
                    stress_scale=slip_calibrator.stress_scale(),
                )
                fill_price = sr.fill_price

            broker_live = (not is_simulation) and bool(config.PAPER_TRADING)
            published = self.signal_producer.publish_signal(
                symbol=buy_leg, side="BUY", quantity=quantity,
                price=fill_price if not broker_live else buy_px,
                order_type="MARKET",
                strategy="STAT_ARB_LONG_ONLY",
                confidence=conf, spread_bps=spread_bps,
                reasoning=(
                    f"SA {pair} z={ps.z_score:.2f} β={ps.hedge_ratio:.3f} "
                    f"hl={ps.half_life:.0f} β-notional — buy underperformer"
                ),
                simulation=not broker_live,
                entry_price=buy_px,
            )
            if not published:
                alerts.emit("ERROR", "STAT_ARB_PUBLISH", f"Failed SA entry {buy_leg}")
                continue

            use_ack = broker_live and getattr(config, "REQUIRE_FILL_ACK", True)
            if use_ack:
                with self._lock:
                    self._pending_entries[buy_leg] = {
                        "market": "CRYPTO",
                        "stop_loss": levels["stop_loss"],
                        "take_profit": levels["take_profit"],
                        "side": "BUY",
                        "qty": quantity,
                        "price": buy_px,
                        "ts": time.time(),
                    }
            else:
                self.portfolio.fill(
                    buy_leg, "BUY", quantity, fill_price, "CRYPTO",
                    stop_loss=levels["stop_loss"], take_profit=levels["take_profit"],
                    live_broker=broker_live,
                )
            self._stat_arb_legs[pair] = buy_leg
            logger.info(
                "STAT_ARB {} BUY {} z={:.2f} qty={:.6g} (close {} if held)",
                pair, buy_leg, ps.z_score, quantity, close_leg,
            )

    def _estimate_adv_usd(self, symbol: str, price: float) -> float:
        """Rough ADV from recent tick notional; floored for impact model stability."""
        floor = float(getattr(config, "IMPACT_ADV_FLOOR_USD", 50_000))
        ticks = self.tick_consumer.get_ticks(symbol)
        if not ticks:
            return floor
        notional = 0.0
        for t in ticks[-2000:]:
            px = t.get("last") or t.get("bid") or price
            vol = float(t.get("volume") or 0)
            if px and vol:
                notional += vol * px
        # Scale sparse window toward a day-like figure
        scaled = notional * max(1.0, 390.0 / max(len(ticks), 1))
        return max(scaled, floor)

    # ── ML label generation ────────────────────────────────────────────────────

    def _tick_ml_labels(self, symbol: str, current_price: float):
        snap = self._price_snapshots[symbol]
        snap.append(current_price)
        if len(snap) >= config.LABEL_LOOKAHEAD_STEPS:
            past_price = snap[0]
            if past_price > 0:
                forward_return = (current_price - past_price) / past_price
                self.ml_model.label_symbol(symbol, forward_return)

    # ── Risk gates ─────────────────────────────────────────────────────────────

    def _daily_loss_ok(self) -> bool:
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).date()
        if self._broker_equity and self._broker_equity > 0:
            equity = self._broker_equity
            capital_base = self._broker_equity  # use live equity for limit pct
            # Day-start should be relative to equity at day open — approximate via tracked start
        else:
            equity = config.PAPER_INITIAL_CAPITAL_USD + self.portfolio.net_pnl(self._latest_prices)
            capital_base = config.PAPER_INITIAL_CAPITAL_USD
        if self._day_utc != today:
            self._day_utc = today
            self._day_start_equity = equity
        day_pnl = equity - self._day_start_equity
        # Prefer day-start equity as base when known
        base = self._day_start_equity if self._day_start_equity > 0 else capital_base
        limit = -base * self.MAX_DAILY_LOSS_PCT
        if day_pnl < limit:
            alerts.emit(
                "CRITICAL", "DAILY_LOSS",
                f"Daily loss limit hit ${day_pnl:.0f} (limit ${limit:.0f})",
                day_pnl=day_pnl, limit=limit, equity=equity,
            )
            logger.warning(
                "Daily loss limit hit: ${:.0f} (limit ${:.0f}) — halting new entries",
                day_pnl, limit,
            )
            return False
        return True

    def _can_open_position(self, symbol: str, new_side: str, market: str = "CRYPTO") -> bool:
        with self._lock:
            if symbol in self._pending_entries or symbol in self._pending_exits:
                return False
        with self.portfolio._lock:
            pos = self.portfolio._positions.get(symbol, {})
            has_position = pos.get("qty", 0) > 0
            current_side = pos.get("side", "FLAT")

        if market == "CRYPTO":
            if new_side == "SELL":
                return False
            if has_position and current_side == "BUY":
                return False
            return self.portfolio.open_count < self.MAX_OPEN_POSITIONS

        if has_position and current_side == new_side:
            return False
        if has_position:
            return True
        return self.portfolio.open_count < self.MAX_OPEN_POSITIONS

    # ── Position monitor ────────────────────────────────────────────────────────

    def _monitor_positions(self):
        now = time.time()
        # Expire stale pending acks (broker never confirmed)
        with self._lock:
            for store_name, store in (("entry", self._pending_entries), ("exit", self._pending_exits)):
                stale = [s for s, m in store.items() if now - m.get("ts", now) > 60]
                for s in stale:
                    alerts.emit(
                        "ERROR", "FILL_ACK_TIMEOUT",
                        f"Pending {store_name} ack timed out for {s}",
                        symbol=s,
                    )
                    logger.warning("Pending ack timed out for {} — clearing", s)
                    store.pop(s, None)
            pending = set(self._pending_exits.keys()) | set(self._pending_entries.keys())

        exits = self.portfolio.check_exits(
            self._latest_prices,
            max_age_hours=self.POSITION_MAX_AGE_HOURS,
            min_hold_minutes=self.MIN_HOLD_MINUTES,
        )
        for ex in exits:
            if ex["symbol"] in pending:
                continue
            self._close_position(ex["symbol"], ex["price"], ex["reason"])

    def _close_position(self, symbol: str, price: float, reason: str):
        with self.portfolio._lock:
            pos = self.portfolio._positions.get(symbol)
            if not pos or pos.get("qty", 0) <= 0:
                return
            qty = pos["qty"]
            avg = pos["avg_price"]
            side = pos["side"]
            live = bool(pos.get("live_broker", False))
            market = pos.get("market", "")

        close_side = "SELL" if side == "BUY" else "BUY"
        broker_live = live and bool(config.PAPER_TRADING)
        use_ack = broker_live and getattr(config, "REQUIRE_FILL_ACK", True)

        exit_px = price
        if not broker_live and getattr(config, "STRESS_SLIPPAGE", True):
            notional = qty * price
            sr = slipped_fill(
                price, close_side,
                spread_bps=5.0,
                atr_pct=0.5,
                order_usd=notional,
                adv_usd=self._estimate_adv_usd(symbol, price),
            )
            exit_px = sr.fill_price

        # Estimate pnl for Kafka payload (finalized on ack for live)
        if side == "BUY":
            est_pnl = qty * (exit_px - avg)
        else:
            est_pnl = qty * (avg - exit_px)
        pnl_str = f"+{est_pnl:.2f}" if est_pnl >= 0 else f"{est_pnl:.2f}"

        published = self.signal_producer.publish_signal(
            symbol      = symbol,
            side        = close_side,
            quantity    = qty,
            price       = exit_px if not broker_live else price,
            order_type  = "MARKET",
            strategy    = f"EXIT_{reason}",
            confidence  = 1.0,
            spread_bps  = 0.0,
            reasoning   = f"Auto-exit: {reason} | entry={avg:.4f} pnl≈{pnl_str}",
            simulation  = not broker_live,
            is_exit     = True,
            entry_price = avg,
            pnl         = est_pnl,
        )
        if not published:
            alerts.emit("ERROR", "EXIT_PUBLISH", f"Exit publish failed for {symbol}")
            logger.error("Exit publish failed for {} — keeping position open", symbol)
            return

        if use_ack:
            with self._lock:
                self._pending_exits[symbol] = {
                    "reason": reason,
                    "qty": qty,
                    "avg": avg,
                    "side": side,
                    "ts": time.time(),
                }
            logger.info(
                "PENDING EXIT {} {} reason={} — awaiting broker ack",
                close_side, symbol, reason,
            )
            return

        # Simulation / no-ack path — close local book immediately
        result = self.portfolio.close_position(symbol, exit_px, reason)
        if not result:
            return
        if hasattr(self._sizer, "record_outcome"):
            self._sizer.record_outcome(result["pnl"], 1.0)
        logger.info(
            "EXIT {} {} reason={} price={:.4f} pnl={}",
            close_side, symbol, reason, exit_px,
            f"+{result['pnl']:.2f}" if result["pnl"] >= 0 else f"{result['pnl']:.2f}",
        )
        record = {
            "timestamp":   time.strftime("%H:%M:%S"),
            "symbol":      symbol,
            "side":        close_side,
            "market":      market or result.get("market", ""),
            "price":       round(exit_px, 6),
            "quantity":    round(result["qty"], 6),
            "confidence":  1.0,
            "reasoning":   f"Auto-exit: {reason} | entry={result['avg_price']:.4f}",
            "entry":       round(result["avg_price"], 6),
            "pnl":         round(result["pnl"], 2),
            "exit_reason": reason,
            "simulation":  not broker_live,
        }
        with self._lock:
            self._recent_signals.append(record)
            if len(self._recent_signals) > 200:
                self._recent_signals = self._recent_signals[-200:]
        self._write_redis_signal(record)

    # ── Quantity helpers ───────────────────────────────────────────────────────

    def _to_quantity(self, usd_amount: float, price: float, market: str) -> float:
        if price <= 0:
            return 0.0
        qty = usd_amount / price
        if market == "CRYPTO":
            return round(qty, 6)
        if market == "FOREX":
            return round(qty / 1000) * 1000
        return max(1.0, round(qty))

    # ── P&L tracking ──────────────────────────────────────────────────────────

    def _update_pnl(self):
        pnl = self.portfolio.net_pnl(self._latest_prices)
        record = {"time": time.strftime("%H:%M:%S"), "pnl": pnl}
        with self._lock:
            self._pnl_history.append(record)
            if len(self._pnl_history) > 500:
                self._pnl_history = self._pnl_history[-500:]
        if self._redis:
            try:
                self._redis.lpush("quant:pnl_history", json.dumps(record))
                self._redis.ltrim("quant:pnl_history", 0, 499)
                positions = self.portfolio.positions_snapshot(self._latest_prices)
                pipe = self._redis.pipeline()
                pipe.delete("quant:positions")
                for pos in positions:
                    pipe.hset("quant:positions", pos["symbol"], json.dumps(pos))
                pipe.execute()
            except Exception:
                pass

    def _write_redis_signal(self, record: dict):
        if not self._redis:
            return
        try:
            self._redis.lpush("quant:signals", json.dumps(record))
            self._redis.ltrim("quant:signals", 0, 199)
        except Exception as e:
            logger.debug("Redis signal write error: {}", e)

    # ── Regime summary log ─────────────────────────────────────────────────────

    def _log_regime_summary(self):
        lines = []
        for sym, detector in self._regime.items():
            prices = [
                t.get("last") or t.get("bid", 0)
                for t in self.tick_consumer.get_ticks(sym)
            ]
            prices = [p for p in prices if p > 0]
            if len(prices) < 50:
                continue
            r = detector.compute(prices)
            if r:
                lines.append(
                    f"  {sym:<14} {r.state.name:<10} vol={r.vol_regime:<7} "
                    f"hurst={r.hurst:.2f} scale={r.position_scale:.2f}"
                )
        if lines:
            logger.info("Regime summary:\n{}", "\n".join(lines))

    def _write_heartbeat(self):
        """Redis liveness for dashboard / ops."""
        if not self._redis:
            return
        try:
            ages = {}
            for sym, px in list(self._latest_prices.items())[:40]:
                ages[sym] = round(px, 6)
            payload = {
                "ts": time.time(),
                "iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "symbols": len(self._latest_prices),
                "open_positions": self.portfolio.open_count,
                "broker_equity": self._broker_equity,
                "kill_switch": self._broker_kill,
                "slip_bias_bps": round(slip_calibrator.mean_bias_bps(), 2),
                "slip_scale": round(slip_calibrator.stress_scale(), 3),
            }
            self._redis.set("quant:heartbeat:python", json.dumps(payload), ex=30)
        except Exception:
            pass

    # ── Main loop ──────────────────────────────────────────────────────────────

    def run(self):
        self.tick_consumer.start()
        self.exec_consumer.start()
        self.pos_consumer.start()
        logger.info("Waiting for market data...")

        dashboard_thread = threading.Thread(
            target=start_dashboard,
            kwargs={
                "pnl_source":      self._pnl_history,
                "signals_source":  self._recent_signals,
                "portfolio":       self.portfolio,
                "prices_source":   self._latest_prices,
                "state_lock":      self._lock,
                "universe_source": self._universe_mgr,
                "scores_source":   self._symbol_scores,
            },
            daemon=True,
        )
        dashboard_thread.start()

        pnl_interval  = 5
        last_pnl_time = 0.0
        last_hb_time  = 0.0

        try:
            while True:
                now = time.time()

                if now - last_hb_time >= 5.0:
                    self._write_heartbeat()
                    last_hb_time = now

                if now - self._last_cs_update >= self.CROSS_SECTIONAL_INTERVAL:
                    try:
                        self._refresh_cross_sectional()
                    except Exception as e:
                        logger.error("Cross-sectional refresh error: {}", e)
                    self._last_cs_update = now

                if now - self._last_stat_arb_ts >= self.STAT_ARB_INTERVAL:
                    try:
                        self._evaluate_stat_arb_pairs()
                    except Exception as e:
                        logger.error("Stat arb evaluation error: {}", e)
                    self._last_stat_arb_ts = now

                for symbol in self.tick_consumer.get_symbols():
                    try:
                        self._evaluate_symbol(symbol)
                    except Exception as e:
                        logger.error("Error evaluating {}: {}", symbol, e)

                if now - self._last_monitor_ts >= self.MONITOR_INTERVAL:
                    try:
                        self._monitor_positions()
                    except Exception as e:
                        logger.error("Position monitor error: {}", e)
                    self._last_monitor_ts = now

                if now - last_pnl_time >= pnl_interval:
                    self._update_pnl()
                    last_pnl_time = now

                if now - self._last_regime_log >= self.REGIME_LOG_INTERVAL:
                    self._log_regime_summary()
                    self._last_regime_log = now

                time.sleep(1)

        except KeyboardInterrupt:
            logger.info("Shutting down Python AI layer...")
        finally:
            self.tick_consumer.stop()
            self.exec_consumer.stop()
            self.pos_consumer.stop()
            self.signal_producer.flush()
            logger.info("Python AI layer stopped.")


if __name__ == "__main__":
    QuantPythonLayer().run()
