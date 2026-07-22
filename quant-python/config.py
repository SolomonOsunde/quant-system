import os
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_TICKS             = "quant.ticks"
TOPIC_SIGNALS           = "quant.signals"
TOPIC_EXECUTIONS        = "quant.executions"
TOPIC_ORDERBOOK         = "quant.orderbook"
TOPIC_POSITIONS         = "quant.positions"

# Claude AI
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL      = "claude-sonnet-4-6"  # fixed: was stale date-stamped ID

# Redis
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# TimescaleDB
DB_URL = os.getenv("DATABASE_URL",
    "postgresql://quant:quant123@localhost:5432/quantdb")

# Signal engine
SIGNAL_LOOKBACK_BARS = 200      # bars for indicator calculation
MIN_CONFIDENCE       = float(os.getenv("MIN_CONFIDENCE", "0.65"))
SIGNAL_COOLDOWN_SEC  = 20       # seconds between signals for same symbol

# ML online learning
LABEL_LOOKAHEAD_STEPS = 5       # evaluation cycles ahead used to compute forward return

# Risk (Python-side soft limits — hard limits enforced in Java RiskEngine)
MAX_POSITION_USD = float(os.getenv("MAX_POSITION_USD", "50000"))
MAX_ORDER_USD    = float(os.getenv("MAX_ORDER_USD", "5000"))
MIN_SPREAD_BPS   = 0.1
MAX_SPREAD_BPS   = 200.0   # high ceiling — commodity sim data has wide artificial spreads

# Paper / live gates
PAPER_TRADING             = os.getenv("PAPER_TRADING", "true").lower() in ("1", "true", "yes")
PAPER_INITIAL_CAPITAL_USD = float(os.getenv("PAPER_INITIAL_CAPITAL_USD", "100000"))
REQUIRE_FILL_ACK          = os.getenv("REQUIRE_FILL_ACK", "true").lower() in ("1", "true", "yes")
# Live testing: reject SIMULATION ticks and refuse stub fills
ALLOW_SIMULATION          = os.getenv("ALLOW_SIMULATION", "false").lower() in ("1", "true", "yes")

# Execution realism
STRESS_SLIPPAGE       = os.getenv("STRESS_SLIPPAGE", "true").lower() in ("1", "true", "yes")
IMPACT_ADV_FLOOR_USD  = float(os.getenv("IMPACT_ADV_FLOOR_USD", "50000"))

# Ops alerting (loguru + Redis quant:alerts; optional webhook)
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "")

# Dashboard
DASHBOARD_HOST = os.getenv("DASHBOARD_HOST", "0.0.0.0")
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "8050"))
