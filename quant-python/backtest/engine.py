"""
Backtest engine — replays historical 1-min bars through the live signal pipeline.

Parity with live:
  - Shared ensemble weights / direction threshold (signals.ensemble)
  - Bar-native momentum & regime lookbacks (calendar 5m/30m/2h/8h)
  - OFI fed every synthetic OHLC tick (or disabled cleanly when no volume)
  - Entry at next-bar open + stress slippage (spread + impact + vol)
  - Path-dependent SL/TP after MIN_HOLD_MINUTES, TIME_STOP at max age
  - Cross-sectional / idiosyncratic momentum across the backtest universe
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from signals.technical import TechnicalSignalEngine
from signals.momentum import MomentumEngine
from signals.regime import RegimeDetector, RegimeState
from signals.order_flow import OrderFlowAnalyzer
from signals.position_sizing import KellyPositionSizer
from signals.slippage import slipped_fill
from signals.ensemble import (
    DIRECTION_THRESHOLD,
    MIN_COMPOSITE,
    MIN_HOLD_MINUTES,
    POSITION_MAX_AGE_HOURS,
    build_ensemble,
    fallback_confidence,
    compute_trade_levels,
    path_exit,
)

# ── Constants ────────────────────────────────────────────────────────────────

TICKS_PER_BAR = 4
WINDOW_BARS = 200
# Enough 1-min bars for MomentumEngine bar-mode 8h lookback (480) + cushion
MOMENTUM_LOOKBACK = 520
DEFAULT_CRYPTO_SPREAD_BPS = 3.0
MAX_HORIZON = 30
TREND_SLOPE_THRESHOLD = 0.005
BAR_SECONDS = 60


@dataclass
class TradeRecord:
    symbol: str
    entry_time: pd.Timestamp
    side: str
    entry_price: float
    composite: float
    confidence: float
    atr_pct: float
    regime: str
    trend: str
    mom_score: float
    return_5b: float
    return_15b: float
    return_30b: float
    hit_5b: bool
    hit_15b: bool
    hit_30b: bool
    pnl: float
    exit_time: Optional[pd.Timestamp] = None
    exit_reason: str = "TIME_STOP"
    exit_price: float = 0.0
    quantity: float = 0.0


def _bars_to_window_ticks(
    opens: np.ndarray, highs: np.ndarray, lows: np.ndarray,
    closes: np.ndarray, vols: np.ndarray, times_ms: np.ndarray,
    symbol: str, market: str,
) -> list[dict]:
    """OHLCV arrays → 4 synthetic ticks/bar preserving H/L for ATR."""
    ticks = []
    bullish = closes >= opens

    for i in range(len(opens)):
        o, h, l, c, v = opens[i], highs[i], lows[i], closes[i], vols[i]
        ts = int(times_ms[i])
        path = (o, l, h, c) if bullish[i] else (o, h, l, c)
        vol4 = max(float(v) / TICKS_PER_BAR, 1e-9)
        for j, price in enumerate(path):
            ticks.append({
                "symbol": symbol,
                "timestamp": ts + j * 15_000,
                "bid": price,
                "ask": price,
                "last": price,
                "volume": vol4,
                "market": market,
                "source": "BACKTEST",
            })
    return ticks


def _bar_ticks(
    o: float, h: float, l: float, c: float, v: float,
    ts_ms: int, symbol: str, market: str,
) -> list[dict]:
    path = (o, l, h, c) if c >= o else (o, h, l, c)
    vol4 = max(float(v) / TICKS_PER_BAR, 1e-9)
    return [{
        "symbol": symbol,
        "timestamp": int(ts_ms) + j * 15_000,
        "bid": price,
        "ask": price,
        "last": price,
        "volume": vol4,
        "market": market,
        "source": "BACKTEST",
    } for j, price in enumerate(path)]


def _apply_spread(price: float, side: str, spread_bps: float, is_entry: bool) -> float:
    half = price * (spread_bps / 2) / 10_000
    if is_entry:
        return price + half if side == "BUY" else price - half
    return price - half if side == "BUY" else price + half


class BacktestEngine:
    """
    market: "CRYPTO" | "FOREX"
    enable_ofi: False for Yahoo FX (volume usually 0) — OFI weight → tech
    spread_bps: scalar default; overridden per symbol via spread_bps_by_symbol
    min_confidence: defaults to config.MIN_CONFIDENCE for live parity
    """

    def __init__(
        self,
        capital: float = 100_000.0,
        market: str = "CRYPTO",
        enable_ofi: bool = True,
        spread_bps: float = DEFAULT_CRYPTO_SPREAD_BPS,
        spread_bps_by_symbol: Optional[dict[str, float]] = None,
        min_composite: float = MIN_COMPOSITE,
        min_confidence: Optional[float] = None,
        direction_threshold: float = DIRECTION_THRESHOLD,
        allow_short: Optional[bool] = None,
    ):
        self.capital = capital
        self.market = market
        self.enable_ofi = enable_ofi
        self.spread_bps = spread_bps
        self.spread_bps_by_symbol = spread_bps_by_symbol or {}
        self.min_composite = min_composite
        self.min_confidence = (
            config.MIN_CONFIDENCE if min_confidence is None else min_confidence
        )
        self.direction_threshold = direction_threshold
        # Crypto Alpaca is long-only; forex/equity may short
        self.allow_short = (market != "CRYPTO") if allow_short is None else allow_short

        self.min_hold_bars = max(1, int(MIN_HOLD_MINUTES))
        self.max_hold_bars = max(self.min_hold_bars, int(POSITION_MAX_AGE_HOURS * 60))

        self.tech_engine = TechnicalSignalEngine()
        self.momentum_engine = MomentumEngine()

    def _spread_for(self, symbol: str) -> float:
        return float(self.spread_bps_by_symbol.get(symbol, self.spread_bps))

    def run(self, bars_by_symbol: dict[str, pd.DataFrame]) -> pd.DataFrame:
        all_records: list[TradeRecord] = []

        # Align CS lookbacks on close series available to every symbol
        close_series = {
            sym: bars["c"].to_numpy(dtype=float)
            for sym, bars in bars_by_symbol.items()
        }

        for symbol, bars in bars_by_symbol.items():
            print(f"  Backtesting {symbol:<14} ({len(bars):,} bars)...", end="", flush=True)
            records = self._backtest_symbol(symbol, bars, close_series)
            all_records.extend(records)
            print(f" {len(records)} signals")

        if not all_records:
            print("No signals generated.")
            return pd.DataFrame()

        return pd.DataFrame([{
            "symbol": r.symbol,
            "entry_time": r.entry_time,
            "side": r.side,
            "entry_price": r.entry_price,
            "composite": r.composite,
            "confidence": r.confidence,
            "atr_pct": r.atr_pct,
            "regime": r.regime,
            "trend": r.trend,
            "mom_score": r.mom_score,
            "return_5b": r.return_5b,
            "return_15b": r.return_15b,
            "return_30b": r.return_30b,
            "hit_5b": r.hit_5b,
            "hit_15b": r.hit_15b,
            "hit_30b": r.hit_30b,
            "hit": r.hit_5b,
            "pnl": r.pnl,
            "exit_time": r.exit_time,
            "exit_reason": r.exit_reason,
            "exit_price": r.exit_price,
            "quantity": r.quantity,
        } for r in all_records])

    def _cs_at(self, symbol: str, bar_idx: int, close_series: dict[str, np.ndarray]) -> tuple[float, float]:
        """Cross-sectional + idiosyncratic scores using closes up to bar_idx (exclusive)."""
        all_prices = {}
        for sym, closes in close_series.items():
            if bar_idx < 20 or bar_idx > len(closes):
                continue
            start = max(0, bar_idx - MOMENTUM_LOOKBACK)
            all_prices[sym] = [float(p) for p in closes[start:bar_idx] if p > 0]

        if symbol not in all_prices or len(all_prices) < 2:
            return 0.0, 0.0

        cs = self.momentum_engine.compute_cross_sectional(
            symbol, all_prices, resolution="bar"
        )
        # Idiosyncratic vs BTC only meaningful for crypto
        idio = 0.0
        if self.market == "CRYPTO":
            idio = self.momentum_engine.compute_idiosyncratic(
                symbol, all_prices, resolution="bar"
            )
        return cs, idio

    def _backtest_symbol(
        self,
        symbol: str,
        bars: pd.DataFrame,
        close_series: dict[str, np.ndarray],
    ) -> list[TradeRecord]:
        records: list[TradeRecord] = []
        regime_det = RegimeDetector(sample_period_seconds=BAR_SECONDS)
        ofi_analyzer = OrderFlowAnalyzer()
        sizer = KellyPositionSizer(self.capital, max_order_usd=config.MAX_ORDER_USD)
        spread_bps = self._spread_for(symbol)

        opens = bars["o"].to_numpy(dtype=float)
        closes = bars["c"].to_numpy(dtype=float)
        highs = bars["h"].to_numpy(dtype=float)
        lows = bars["l"].to_numpy(dtype=float)
        volumes = bars["v"].to_numpy(dtype=float)
        bar_times = (pd.to_datetime(bars["t"]).astype("int64") // 1_000_000).to_numpy()

        n_bars = len(closes)
        # Need room for next-bar entry + max hold / horizon measurement
        last_eval = n_bars - max(self.max_hold_bars, MAX_HORIZON) - 2
        in_position_until = -1
        cs_cache = (0.0, 0.0)
        cs_cache_bar = -999

        # Warm OFI with the initial window so compute() has ≥50 ticks
        if self.enable_ofi and last_eval > WINDOW_BARS:
            warm = _bars_to_window_ticks(
                opens[:WINDOW_BARS], highs[:WINDOW_BARS], lows[:WINDOW_BARS],
                closes[:WINDOW_BARS], volumes[:WINDOW_BARS], bar_times[:WINDOW_BARS],
                symbol, self.market,
            )
            for t in warm:
                ofi_analyzer.update(t)

        for b in range(WINDOW_BARS, last_eval):
            # Feed this bar's synthetic ticks into OFI (causal, incremental)
            if self.enable_ofi:
                for t in _bar_ticks(
                    opens[b], highs[b], lows[b], closes[b], volumes[b],
                    bar_times[b], symbol, self.market,
                ):
                    ofi_analyzer.update(t)

            if b <= in_position_until:
                continue

            start = b - WINDOW_BARS
            window = _bars_to_window_ticks(
                opens[start:b + 1], highs[start:b + 1], lows[start:b + 1],
                closes[start:b + 1], volumes[start:b + 1], bar_times[start:b + 1],
                symbol, self.market,
            )

            tech = self.tech_engine.compute(symbol, window)
            if tech is None:
                continue
            # Live does not require tech.direction ≠ 0 — mom/ofi can drive the trade

            mom_start = max(0, b + 1 - MOMENTUM_LOOKBACK)
            prices_list = [float(p) for p in closes[mom_start:b + 1] if p > 0]
            if len(prices_list) < MomentumEngine.MIN_BARS:
                continue

            regime = regime_det.compute(prices_list)
            if regime and regime.state == RegimeState.CRISIS:
                continue

            mom = self.momentum_engine.compute_ts_momentum(
                prices_list, skip_recent=0, resolution="bar"
            )
            ofi = ofi_analyzer.compute(symbol) if self.enable_ofi else None

            # CS/idio refresh ~ every 15 bars (matches live ~30s cadence on 1m)
            if b - cs_cache_bar >= 15:
                cs_cache = self._cs_at(symbol, b + 1, close_series)
                cs_cache_bar = b
            cs_mom, idio_mom = cs_cache

            ensemble = build_ensemble(
                tech, (0.0, 0.0), ofi, mom, cs_mom, idio_mom, regime,
                enable_ofi=self.enable_ofi,
                enable_ml=False,  # online ML not trained in offline replay
                direction_threshold=self.direction_threshold,
            )
            direction = ensemble["direction"]
            composite = ensemble["composite"]
            if direction == 0 or abs(composite) < self.min_composite:
                continue

            # Crypto long-only: skip short entries (matches live Alpaca policy)
            if direction < 0 and not self.allow_short:
                continue

            atr_pct = tech.signals.get("atr_pct", 0.5)
            confidence = fallback_confidence(
                composite, tech.confidence, 0.0, regime, ofi
            )
            if confidence < self.min_confidence:
                continue

            side = "BUY" if direction > 0 else "SELL"

            # ── Next-bar open fill (no look-ahead on same-bar close) ─────────
            entry_bar = b + 1
            raw_entry = float(opens[entry_bar])
            if raw_entry <= 0:
                continue

            # Provisional size at mid, then stress-slip the fill
            rv = regime.realized_vol if regime and regime.realized_vol > 0 else max(atr_pct / 100.0 * np.sqrt(365 * 24 * 60), 0.01)
            pos_scale = regime.position_scale if regime else 1.0
            vol_window = volumes[max(0, entry_bar - 390):entry_bar]
            adv_usd = float(np.sum(vol_window) * raw_entry) if len(vol_window) else float(
                getattr(config, "IMPACT_ADV_FLOOR_USD", 50_000)
            )
            adv_usd = max(adv_usd, float(getattr(config, "IMPACT_ADV_FLOOR_USD", 50_000)))
            sizing = sizer.compute(
                confidence, raw_entry, rv, spread_bps, 0.0, adv_usd=adv_usd,
            )
            order_usd = sizing.recommended_usd * pos_scale
            if order_usd <= 0:
                continue

            vpin = float(getattr(ofi, "vpin", 0) or 0) if ofi else 0.0
            vol_ratio = float(tech.signals.get("volume_ratio", 1.0) or 1.0)
            if getattr(config, "STRESS_SLIPPAGE", True):
                sr_in = slipped_fill(
                    raw_entry, side,
                    spread_bps=spread_bps,
                    atr_pct=float(atr_pct or 0.5),
                    order_usd=order_usd,
                    adv_usd=adv_usd,
                    vpin=vpin,
                    volume_ratio=vol_ratio,
                )
                fill_entry = sr_in.fill_price
            else:
                fill_entry = _apply_spread(raw_entry, side, spread_bps, is_entry=True)

            levels = compute_trade_levels(fill_entry, side, atr_pct, self.market)
            quantity = order_usd / fill_entry
            if quantity <= 0:
                continue

            exit_bar, raw_exit, exit_reason = path_exit(
                side, entry_bar, highs, lows, closes,
                levels["stop_loss"], levels["take_profit"],
                min_hold_bars=self.min_hold_bars,
                max_hold_bars=self.max_hold_bars,
            )
            if getattr(config, "STRESS_SLIPPAGE", True):
                exit_side = "SELL" if side == "BUY" else "BUY"
                sr_out = slipped_fill(
                    raw_exit, exit_side,
                    spread_bps=spread_bps,
                    atr_pct=float(atr_pct or 0.5),
                    order_usd=quantity * raw_exit,
                    adv_usd=adv_usd,
                    vpin=vpin,
                    volume_ratio=vol_ratio,
                )
                fill_exit = sr_out.fill_price
            else:
                fill_exit = _apply_spread(raw_exit, side, spread_bps, is_entry=False)
            if side == "BUY":
                pnl = quantity * (fill_exit - fill_entry)
            else:
                pnl = quantity * (fill_entry - fill_exit)

            # Research horizons from fill entry (still useful diagnostics)
            def _fwd(n: int) -> float:
                idx = min(entry_bar + n, n_bars - 1)
                return (float(closes[idx]) - fill_entry) / fill_entry

            ret5, ret15, ret30 = _fwd(5), _fwd(15), _fwd(30)
            hit5 = (ret5 > 0) if side == "BUY" else (ret5 < 0)
            hit15 = (ret15 > 0) if side == "BUY" else (ret15 < 0)
            hit30 = (ret30 > 0) if side == "BUY" else (ret30 < 0)

            ref = float(closes[max(0, b - 20)])
            slope = (float(closes[b]) - ref) / ref if ref > 0 else 0.0
            trend = (
                "UP" if slope > TREND_SLOPE_THRESHOLD
                else "DOWN" if slope < -TREND_SLOPE_THRESHOLD
                else "FLAT"
            )

            if hasattr(sizer, "record_outcome"):
                sizer.record_outcome(pnl, confidence)

            records.append(TradeRecord(
                symbol=symbol,
                entry_time=pd.Timestamp(int(bar_times[entry_bar]), unit="ms"),
                side=side,
                entry_price=fill_entry,
                composite=composite,
                confidence=confidence,
                atr_pct=atr_pct,
                regime=regime.state.name if regime else "UNKNOWN",
                trend=trend,
                mom_score=mom.ts_momentum if mom else 0.0,
                return_5b=ret5,
                return_15b=ret15,
                return_30b=ret30,
                hit_5b=hit5,
                hit_15b=hit15,
                hit_30b=hit30,
                pnl=pnl,
                exit_time=pd.Timestamp(int(bar_times[exit_bar]), unit="ms"),
                exit_reason=exit_reason,
                exit_price=fill_exit,
                quantity=quantity,
            ))
            in_position_until = exit_bar

        return records
