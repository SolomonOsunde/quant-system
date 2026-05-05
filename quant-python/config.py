import os
from dotenv import load_dotenv

load_dotenv()

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_TICKS             = "quant.ticks"
TOPIC_SIGNALS           = "quant.signals"
TOPIC_EXECUTIONS        = "quant.executions"
TOPIC_ORDERBOOK         = "quant.orderbook"

# Claude AI
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL      = "claude-sonnet-4-20250514"

# Redis
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# TimescaleDB
DB_URL = os.getenv("DATABASE_URL",
    "postgresql://quant:quant123@localhost:5432/quantdb")

# Signal engine
SIGNAL_LOOKBACK_BARS  = 200     # bars for indicator calculation
MIN_CONFIDENCE        = 0.60    # minimum signal confidence to publish
SIGNAL_COOLDOWN_SEC   = 30      # seconds between signals for same symbol

# Risk (Python-side soft limits — hard limits in Java)
MAX_POSITION_USD  = 50_000
MAX_ORDER_USD     = 5_000
MIN_SPREAD_BPS    = 0.1
MAX_SPREAD_BPS    = 4.0

# Dashboard
DASHBOARD_HOST = "0.0.0.0"
DASHBOARD_PORT = 8050
