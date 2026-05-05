import anthropic
import json
from loguru import logger
import config


class ClaudeReasoningEngine:
    """
    Uses Claude API to apply contextual reasoning on top of
    quantitative signals before a trade decision is made.

    Claude acts as a senior quant analyst — given the raw signals,
    market context, and recent price action, it returns a structured
    trade decision with reasoning.
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
            logger.warning("ANTHROPIC_API_KEY not set — Claude reasoning disabled.")
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
        """
        Ask Claude to reason about a potential trade.
        Returns a decision dict.
        """
        if self._client is None:
            # Fallback: trust technical + ML signals directly
            return self._fallback_decision(
                technical_direction, technical_confidence, ml_prediction, ml_confidence
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
                technical_direction, technical_confidence, ml_prediction, ml_confidence
            )
        except Exception as e:
            logger.error("Claude API error for {}: {}", symbol, e)
            return self._fallback_decision(
                technical_direction, technical_confidence, ml_prediction, ml_confidence
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
        self, direction: int, tech_conf: float,
        ml_pred: float, ml_conf: float
    ) -> dict:
        """Decision without Claude — blend technical and ML signals."""
        combined_conf = (tech_conf + ml_conf) / 2
        ml_dir = 1 if ml_pred > 0 else -1

        # Both signals must agree
        if direction == ml_dir and combined_conf >= config.MIN_CONFIDENCE:
            decision = "BUY" if direction > 0 else "SELL"
        else:
            decision = "HOLD"

        return {
            "decision":           decision,
            "confidence":         round(combined_conf, 4),
            "reasoning":          "Fallback: technical + ML agreement",
            "risk_flags":         [],
            "suggested_size_pct": round(combined_conf * 0.5, 2),
        }
