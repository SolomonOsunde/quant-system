# Polyglot Quant Trading System

A professional-grade algorithmic trading system using:
- **Java** — low-latency market data ingestion, order book, execution engine, pre-trade risk
- **Python** — AI signal generation, ML ensemble models, Claude AI reasoning, live dashboard
- **Kafka** — async message bus connecting both layers
- **TimescaleDB** — tick and trade history
- **Redis** — fast shared state cache

## Markets Supported
- Forex (FX)
- Equities
- Crypto
- Commodities / Futures

## Architecture
```
Market Feeds → Java Ingestion → Kafka → Python Signal Engine → Claude AI
                                  ↑              ↓
                           Java Execution ← Kafka ← ML Decision
```

## Prerequisites
- Java 17+
- Python 3.11+
- Docker + Docker Compose
- Maven 3.9+

## Quick Start

### 1. Start infrastructure
```bash
cd infra
docker-compose up -d
```

### 2. Start Java core
```bash
cd quant-java
mvn clean package -q
java -jar target/quant-java.jar
```

### 3. Start Python AI layer
```bash
cd quant-python
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
python main.py
```

### 4. View dashboard
Open http://localhost:8050

## Configuration
- Java config: `quant-java/src/main/resources/application.properties`
- Python config: `quant-python/config.py`
- Kafka topics: `infra/kafka-topics.sh`

## Project Structure
```
quant-system/
├── quant-java/          # Java low-latency core
│   └── src/main/java/com/quant/
│       ├── ingestion/   # Market data WebSocket feeds
│       ├── orderbook/   # L2 order book engine
│       ├── execution/   # Order routing + FIX
│       ├── risk/        # Pre-trade risk checks
│       └── kafka/       # Kafka producer
├── quant-python/        # Python AI layer
│   ├── signals/         # Technical + microstructure signals
│   ├── ml/              # XGBoost/LightGBM ensemble
│   ├── ai/              # Claude API reasoning engine
│   ├── kafka/           # Kafka consumer/producer
│   └── dashboard/       # Live P&L dashboard (Dash)
├── infra/               # Docker Compose, Kafka setup
└── docs/                # Architecture diagrams
```
