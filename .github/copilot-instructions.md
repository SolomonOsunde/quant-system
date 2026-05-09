# Quant System Copilot Instructions

## Architecture Overview
This is a polyglot algorithmic trading system with Java for low-latency components and Python for AI-driven signals. Data flows: Market feeds → Java ingestion → Kafka → Python signal engine (technical + ML + Claude AI) → Kafka → Java execution engine → broker routing.

Key components:
- **Java core** (`quant-java/`): WebSocket market data ingestion, order book management, risk engine, execution engine
- **Python AI layer** (`quant-python/`): Consumes ticks, computes signals, publishes trade decisions
- **Infrastructure** (`infra/`): Docker Compose with Kafka, TimescaleDB, Redis

## Critical Workflows
- **Start infrastructure**: `cd infra && docker-compose up -d` (starts Kafka, DB, Redis)
- **Build Java**: `cd quant-java && mvn clean package -q` (creates fat JAR with main class `com.quant.QuantSystem`)
- **Run Java core**: `java -jar quant-java/target/quant-java.jar` (starts feeds, order book, execution listener)
- **Setup Python**: `cd quant-python && python -m venv venv && source venv/bin/activate && pip install -r requirements.txt`
- **Run Python AI**: `python main.py` (consumes ticks, evaluates signals every 5s per symbol)
- **View dashboard**: Open http://localhost:8050 (Dash app for P&L, positions)

## Project Conventions
- **Configuration**: Java uses `application.properties` (Kafka topics: `quant.ticks`, `quant.signals`, `quant.executions`); Python uses `config.py` with env vars
- **Market data**: `TickData` POJO with enum `Market {FOREX, EQUITY, CRYPTO, COMMODITY}`; feeds extend `BaseMarketFeed`, publish to Kafka and update order book
- **Signals**: JSON format `{"symbol": "BTCUSDT", "side": "BUY", "quantity": 0.01, "price": 42000.0, "orderType": "LIMIT", "strategy": "ML_ENSEMBLE", "confidence": 0.82, "spreadBps": 1.2}`
- **Risk checks**: Synchronous pre-trade validation in `RiskEngine` (position limits, kill switch, spread filters)
- **Python signals**: `SignalResult` dataclass with direction (-1/0/+1), confidence (0-1); rolling tick windows (500 ticks) for OHLCV resampling
- **ML ensemble**: XGBoost + LightGBM voting classifier; features from technicals (RSI, MACD, BB); retrains every 1000 samples
- **Claude reasoning**: Structured JSON output for trade decisions; system prompt enforces conservative, multi-factor confirmation

## Integration Patterns
- **Kafka messaging**: Async decoupling between Java/Python; use `confluent-kafka` in Python, `kafka-clients` in Java
- **WebSocket feeds**: Each feed (e.g., `CryptoFeed`) connects to exchange APIs (Binance, Alpaca), parses JSON, creates `TickData`
- **Database**: TimescaleDB hypertables for `ticks`, `trades`, `signals`; use SQLAlchemy in Python for queries
- **Caching**: Redis for shared state (positions, P&L); Jedis in Java, `redis` lib in Python

## Key Files
- `quant-java/src/main/java/com/quant/QuantSystem.java`: Main entry, wires components
- `quant-python/main.py`: Python orchestrator, evaluates signals per symbol
- `quant-java/src/main/java/com/quant/ingestion/MarketDataManager.java`: Starts all feeds
- `quant-python/signals/technical.py`: Computes RSI/MACD/BB signals from OHLCV
- `quant-python/ml/ensemble.py`: ML model training/prediction pipeline
- `quant-python/ai/claude_engine.py`: Claude API integration for reasoning
- `infra/docker-compose.yml`: Infrastructure setup</content>
<parameter name="filePath">/Users/solomonosunde/Documents/quant-system/.github/copilot-instructions.md