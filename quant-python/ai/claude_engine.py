import anthropic
import json
from loguru import logger
import config


class ClaudeReasoningEngine:
    """
    Uses Claude API to apply contextual reasoning on top of quantitative signals.
    Falls back to rule-based human-readable reasoning when API key is not set.
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
    ) -> dict:
        if self._client is None:
            return self._fallback_decision(
                symbol, technical_direction, technical_confidence,
                ml_prediction, ml_confidence, technical_signals
            )

        prompt = self._build_prompt(
            symbol, market, technical_signals, technical_direction,
            technical_confidence, ml_prediction, ml_confidence,
            current_price, spread_bps
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
            return self._fallback_decision(
                symbol, technical_direction, technical_confidence,
                ml_prediction, ml_confidence, technical_signals
            )
        except Exception as e:
            logger.error("Claude API error for {}: {}", symbol, e)
            return self._fallback_decision(
                symbol, technical_direction, technical_confidence,
                ml_prediction, ml_confidence, technical_signals
            )

    def _build_prompt(
        self, symbol, market, signals, direction, tech_conf,
        ml_pred, ml_conf, price, spread_bps
    ) -> str:
        dir_str = "BULLISH" if direction > 0 else ("BEARISH" if direction < 0 else "NEUTRAL")
        return f"""Analyze this intraday trade opportunity:

Symbol: {symbol}
Market: {market}
Current Price: {price:.6f}
Spread: {spread_bps:.2f} bps

Technical Analysis:
- Direction: {dir_str}
- Confidence: {tech_conf:.2f}
- RSI: {signals.get('rsi', 'N/A')}
- MACD histogram: {signals.get('macd_hist', 'N/A')}
- Bollinger %B: {signals.get('bb_pct_b', 'N/A')}
- EMA crossover: {signals.get('ema_cross', 'N/A')}
- ATR%: {signals.get('atr_pct', 'N/A')}
- Volume ratio: {signals.get('volume_ratio', 'N/A')}
- Score: {signals.get('score', 'N/A')}

ML Ensemble:
- Predicted direction: {"UP" if ml_pred > 0 else "DOWN"} ({ml_pred:.4f})
- ML confidence: {ml_conf:.2f}

Should we trade this? Return JSON only."""

    def _fallback_decision(
        self,
        symbol: str,
        direction: int,
        tech_conf: float,
        ml_pred: float,
        ml_conf: float,
        signals: dict,
    ) -> dict:
        """Rule-based decision with rich human-readable reasoning."""
        combined_conf = (tech_conf + ml_conf) / 2
        ml_dir = 1 if ml_pred > 0 else -1

        if direction == ml_dir and combined_conf >= config.MIN_CONFIDENCE:
            decision = "BUY" if direction > 0 else "SELL"
        else:
            decision = "HOLD"

        reasoning = self._build_reasoning(
            symbol, direction, ml_pred, ml_conf, combined_conf, signals, decision
        )

        risk_flags = []
        atr_pct = signals.get("atr_pct", 0)
        if atr_pct and atr_pct > 2.0:
            risk_flags.append(f"High volatility: ATR {atr_pct:.1f}%")
        vol_ratio = signals.get("volume_ratio", 1.0)
        if vol_ratio and vol_ratio < 0.5:
            risk_flags.append("Low volume — thin market")
        if direction != ml_dir:
            risk_flags.append("Technical and ML signals disagree")

        return {
            "decision":           decision,
            "confidence":         round(combined_conf, 4),
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
        signals: dict,
        decision: str,
    ) -> str:
        parts = []

        rsi = signals.get("rsi")
        if rsi is not None:
            if rsi < 25:
                parts.append(f"RSI extremely oversold at {rsi:.1f} — strong reversal signal")
            elif rsi < 35:
                parts.append(f"RSI oversold at {rsi:.1f} — bullish pressure building")
            elif rsi > 75:
                parts.append(f"RSI extremely overbought at {rsi:.1f} — reversal likely")
            elif rsi > 65:
                parts.append(f"RSI overbought at {rsi:.1f} — bearish pressure building")
            else:
                parts.append(f"RSI neutral at {rsi:.1f}")

        macd_hist = signals.get("macd_hist")
        if macd_hist is not None:
            if macd_hist > 0:
                parts.append(f"MACD histogram positive ({macd_hist:+.5f}) — bullish momentum")
            else:
                parts.append(f"MACD histogram negative ({macd_hist:+.5f}) — bearish momentum")

        bb = signals.get("bb_pct_b")
        if bb is not None:
            if bb < 0.05:
                parts.append("price breached lower Bollinger Band — deep oversold, mean-reversion BUY zone")
            elif bb > 0.95:
                parts.append("price breached upper Bollinger Band — deep overbought, mean-reversion SELL zone")
            elif bb < 0.2:
                parts.append(f"price near lower Bollinger Band (BB%B={bb:.2f}) — oversold")
            elif bb > 0.8:
                parts.append(f"price near upper Bollinger Band (BB%B={bb:.2f}) — overbought")

        ema = signals.get("ema_cross")
        if ema is not None:
            if ema > 0:
                parts.append("EMA9 above EMA21 — short-term uptrend confirmed")
            else:
                parts.append("EMA9 below EMA21 — short-term downtrend confirmed")

        vol_ratio = signals.get("volume_ratio")
        if vol_ratio is not None:
            if vol_ratio > 2.0:
                parts.append(f"volume {vol_ratio:.1f}x average — strong momentum confirmation")
            elif vol_ratio > 1.3:
                parts.append(f"volume {vol_ratio:.1f}x average — above-average activity")

        ml_dir_str = "UP" if ml_pred > 0 else "DOWN"
        parts.append(
            f"ML ensemble predicts {ml_dir_str} with {ml_conf:.0%} confidence"
        )

        if decision in ("BUY", "SELL"):
            parts.append(
                f"Technical and ML signals agree → {decision} "
                f"(combined confidence {combined_conf:.0%})"
            )
        else:
            parts.append("Technical and ML signals conflict or confidence too low → HOLD")

        return ". ".join(parts) + "."
