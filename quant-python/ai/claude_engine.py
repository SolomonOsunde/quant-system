import anthropic
import json
from loguru import logger
import config


class ClaudeReasoningEngine:
    """
    Uses Claude API for contextual reasoning. Falls back to rule-based logic
    when the API key is not set. The fallback uses the full 7-factor composite
    score as its primary confidence driver rather than tech+ML average only.
    """

    SYSTEM_PROMPT = """You are a senior quantitative analyst at a hedge fund.
You receive structured market signals and must decide whether to trade.

Rules:
- Only approve trades with strong multi-factor confirmation
- Always flag if spread is too wide or volatility is extreme
- Consider market session (Asian/London/NY overlap for FX)
- Be conservative — a missed trade is better than a bad trade
- Return ONLY valid JSON, no markdown, no explanation outside the JSON

Output format:
{
  "decision": "BUY" | "SELL" | "HOLD",
  "confidence": 0.0-1.0,
  "reasoning": "concise explanation under 100 words",
  "risk_flags": ["list of any concerns"],
  "suggested_size_pct": 0.0-1.0
}"""

    def __init__(self):
        if not config.ANTHROPIC_API_KEY:
            logger.warning("ANTHROPIC_API_KEY not set — using rule-based reasoning.")
            self._client = None
        else:
            self._client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
            logger.info("Claude reasoning engine initialized (model={})", config.CLAUDE_MODEL)

    def reason(
        self,
        symbol: str,
        market: str,
        technical_signals: dict,
        technical_direction: int,
        technical_confidence: float,
        ml_prediction: float,
        ml_confidence: float,
        current_price: float,
        spread_bps: float,
        # Extended context — all optional so old callers still work
        composite_score: float = 0.0,
        ofi=None,
        regime=None,
        mom_result=None,
        cs_momentum: float = 0.0,
        idio_momentum: float = 0.0,
        ms_features: dict = None,
    ) -> dict:
        if self._client is None:
            return self._fallback_decision(
                symbol, technical_direction, technical_confidence,
                ml_prediction, ml_confidence, technical_signals,
                composite_score=composite_score,
                ofi=ofi,
                regime=regime,
            )

        prompt = self._build_prompt(
            symbol, market, technical_signals, technical_direction,
            technical_confidence, ml_prediction, ml_confidence,
            current_price, spread_bps, composite_score, ofi, regime,
        )

        try:
            response = self._client.messages.create(
                model=config.CLAUDE_MODEL,
                max_tokens=400,
                system=self.SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = response.content[0].text.strip()
            decision = json.loads(raw)
            logger.debug("Claude decision for {}: {}", symbol, decision.get("decision"))
            return decision

        except json.JSONDecodeError as e:
            logger.error("Claude returned invalid JSON for {}: {}", symbol, e)
        except Exception as e:
            logger.error("Claude API error for {}: {}", symbol, e)

        return self._fallback_decision(
            symbol, technical_direction, technical_confidence,
            ml_prediction, ml_confidence, technical_signals,
            composite_score=composite_score, ofi=ofi, regime=regime,
        )

    def _build_prompt(
        self, symbol, market, signals, direction, tech_conf,
        ml_pred, ml_conf, price, spread_bps,
        composite=0.0, ofi=None, regime=None,
    ) -> str:
        dir_str  = "BULLISH" if direction > 0 else ("BEARISH" if direction < 0 else "NEUTRAL")
        ofi_str  = f"{ofi.direction:+.2f} (conf={ofi.confidence:.2f})" if ofi else "N/A"
        reg_str  = regime.state.name if regime else "N/A"
        return f"""Analyze this intraday trade opportunity:

Symbol: {symbol}  Market: {market}  Price: {price:.6f}  Spread: {spread_bps:.1f}bps

Technical: {dir_str} conf={tech_conf:.2f} | RSI={signals.get('rsi','N/A')} MACD={signals.get('macd_hist','N/A')} BB%B={signals.get('bb_pct_b','N/A')} EMA_cross={signals.get('ema_cross','N/A')} ATR%={signals.get('atr_pct','N/A')} Vol_ratio={signals.get('volume_ratio','N/A')}
ML: {"UP" if ml_pred > 0 else "DOWN"} ({ml_pred:.3f}) conf={ml_conf:.2f}
OFI (order flow imbalance): {ofi_str}
Regime: {reg_str}
Composite score (7-factor ensemble): {composite:+.4f}

Should we trade? Return JSON only."""

    def _fallback_decision(
        self,
        symbol: str,
        direction: int,
        tech_conf: float,
        ml_pred: float,
        ml_conf: float,
        signals: dict,
        composite_score: float = 0.0,
        ofi=None,
        regime=None,
    ) -> dict:
        """
        Rule-based decision that uses the full 7-factor composite as primary driver.

        Confidence breakdown:
          55% — composite-based (all 7 signals regime-weighted)
          30% — technical confidence
          15% — ML confidence (zero weight until model is trained)

        This produces meaningful confidence values even in a sideways market,
        as long as multiple signals agree on direction.
        """
        ml_trained = ml_conf > 0.0
        ml_dir     = 1 if ml_pred > 0 else -1

        # ── Composite confidence (primary) ─────────────────────────────────────
        # composite > 0.08 is the direction threshold; scale 0→1 over 0.08→0.38
        abs_comp      = abs(composite_score)
        composite_conf = min(max(abs_comp - 0.08, 0.0) / 0.30, 1.0)

        # Use composite direction when composite is directional; fall back to tech
        if abs_comp > 0.08:
            primary_direction = 1 if composite_score > 0 else -1
        else:
            primary_direction = direction

        # ── ML contribution (only when trained and aligned) ───────────────────
        ml_contribution = 0.0
        if ml_trained and primary_direction != 0:
            if ml_dir == primary_direction:
                ml_contribution = ml_conf
            # ML disagreement doesn't reduce confidence — ensemble already accounts for it

        # ── OFI agreement bonus ────────────────────────────────────────────────
        ofi_bonus = 0.0
        if ofi and primary_direction != 0 and ofi.confidence > 0.3:
            if (ofi.direction > 0) == (primary_direction > 0):
                ofi_bonus = 0.06  # OFI confirms direction

        # Regime position_scale is applied to USD size in main.py — not here
        combined_conf = (
            0.55 * composite_conf +
            0.30 * tech_conf      +
            0.15 * ml_contribution +
            ofi_bonus
        )

        combined_conf = round(min(combined_conf, 1.0), 4)

        if primary_direction != 0 and combined_conf >= config.MIN_CONFIDENCE:
            decision = "BUY" if primary_direction > 0 else "SELL"
        else:
            decision = "HOLD"

        reasoning = self._build_reasoning(
            symbol, primary_direction, ml_pred, ml_conf,
            combined_conf, composite_score, signals, decision,
            regime=regime,
        )

        risk_flags = []
        atr_pct = signals.get("atr_pct", 0)
        if atr_pct and atr_pct > 2.0:
            risk_flags.append(f"High volatility: ATR {atr_pct:.1f}%")
        if signals.get("volume_ratio", 1.0) < 0.5:
            risk_flags.append("Low volume — thin market")
        if ml_trained and ml_dir != primary_direction:
            risk_flags.append("ML disagrees with composite direction")
        if regime and hasattr(regime, "state") and regime.state.name == "SIDEWAYS":
            risk_flags.append("Sideways regime — reduced position scale")

        return {
            "decision":           decision,
            "confidence":         combined_conf,
            "reasoning":          reasoning,
            "risk_flags":         risk_flags,
            "suggested_size_pct": round(combined_conf * 0.6, 2),
        }

    def _build_reasoning(
        self,
        symbol: str,
        direction: int,
        ml_pred: float,
        ml_conf: float,
        combined_conf: float,
        composite: float,
        signals: dict,
        decision: str,
        regime=None,
    ) -> str:
        parts = []

        rsi = signals.get("rsi")
        if rsi is not None:
            if rsi < 25:
                parts.append(f"RSI {rsi:.1f} — extremely oversold")
            elif rsi < 35:
                parts.append(f"RSI {rsi:.1f} — oversold")
            elif rsi > 75:
                parts.append(f"RSI {rsi:.1f} — extremely overbought")
            elif rsi > 65:
                parts.append(f"RSI {rsi:.1f} — overbought")
            else:
                parts.append(f"RSI {rsi:.1f} neutral")

        macd = signals.get("macd_hist")
        if macd is not None:
            parts.append(f"MACD hist {macd:+.5f} ({'bullish' if macd > 0 else 'bearish'})")

        bb = signals.get("bb_pct_b")
        if bb is not None:
            if bb < 0.05:
                parts.append("BB%B <0.05 — deep oversold")
            elif bb > 0.95:
                parts.append("BB%B >0.95 — deep overbought")
            elif bb < 0.20:
                parts.append(f"BB%B {bb:.2f} — near lower band")
            elif bb > 0.80:
                parts.append(f"BB%B {bb:.2f} — near upper band")

        ema = signals.get("ema_cross")
        if ema is not None:
            parts.append(f"EMA cross {ema:+.5f} ({'uptrend' if ema > 0 else 'downtrend'})")

        vol = signals.get("volume_ratio")
        if vol and vol > 1.3:
            parts.append(f"volume {vol:.1f}x avg")

        parts.append(f"composite={composite:+.3f}")

        if regime:
            parts.append(f"regime={regime.state.name} scale={regime.position_scale:.2f}")

        ml_dir_str = "UP" if ml_pred > 0 else "DOWN"
        parts.append(f"ML={ml_dir_str} conf={ml_conf:.0%}")

        action = {"BUY": "LONG", "SELL": "SHORT"}.get(decision, "HOLD")
        parts.append(f"→ {action} conf={combined_conf:.0%}")

        return " | ".join(parts)
