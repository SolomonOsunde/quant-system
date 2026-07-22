-- Tick data hypertable
CREATE TABLE IF NOT EXISTS ticks (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    market      TEXT NOT NULL,
    bid         DOUBLE PRECISION,
    ask         DOUBLE PRECISION,
    last        DOUBLE PRECISION,
    volume      DOUBLE PRECISION,
    source      TEXT
);
SELECT create_hypertable('ticks', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS ticks_symbol_time_idx ON ticks (symbol, time DESC);

-- Trades / fills
CREATE TABLE IF NOT EXISTS trades (
    time        TIMESTAMPTZ NOT NULL,
    order_id    TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    side        TEXT NOT NULL,
    quantity    DOUBLE PRECISION,
    price       DOUBLE PRECISION,
    pnl         DOUBLE PRECISION,
    strategy    TEXT
);
SELECT create_hypertable('trades', 'time', if_not_exists => TRUE);

-- Signals log
CREATE TABLE IF NOT EXISTS signals (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    value       DOUBLE PRECISION,
    confidence  DOUBLE PRECISION,
    source      TEXT
);
SELECT create_hypertable('signals', 'time', if_not_exists => TRUE);

-- ── Live instrument universe (no hardcoded pairs in app code) ───────────────
CREATE TABLE IF NOT EXISTS instruments (
    id              SERIAL PRIMARY KEY,
    symbol          TEXT NOT NULL,          -- internal: EUR/USD, BTC/USD
    market          TEXT NOT NULL,          -- FOREX | CRYPTO | EQUITY | COMMODITY
    broker_symbol   TEXT NOT NULL,          -- OANDA EUR_USD / Alpaca BTCUSD
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    priority        INT NOT NULL DEFAULT 100,
    notes           TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (market, symbol)
);
CREATE INDEX IF NOT EXISTS instruments_enabled_idx
    ON instruments (market, enabled, priority);

-- Stat-arb cointegration pairs (legs reference instruments.symbol)
CREATE TABLE IF NOT EXISTS stat_arb_pairs (
    id          SERIAL PRIMARY KEY,
    leg1        TEXT NOT NULL,
    leg2        TEXT NOT NULL,
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    priority    INT NOT NULL DEFAULT 100,
    notes       TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (leg1, leg2)
);

-- Seed FOREX (OANDA instruments)
INSERT INTO instruments (symbol, market, broker_symbol, enabled, priority) VALUES
    ('EUR/USD', 'FOREX', 'EUR_USD', TRUE, 10),
    ('GBP/USD', 'FOREX', 'GBP_USD', TRUE, 20),
    ('USD/JPY', 'FOREX', 'USD_JPY', TRUE, 30),
    ('AUD/USD', 'FOREX', 'AUD_USD', TRUE, 40),
    ('USD/CAD', 'FOREX', 'USD_CAD', TRUE, 50),
    ('USD/CHF', 'FOREX', 'USD_CHF', TRUE, 60),
    ('EUR/GBP', 'FOREX', 'EUR_GBP', TRUE, 70),
    ('EUR/JPY', 'FOREX', 'EUR_JPY', TRUE, 80)
ON CONFLICT (market, symbol) DO NOTHING;

-- Seed CRYPTO (Alpaca slash symbols; broker_symbol without slash)
INSERT INTO instruments (symbol, market, broker_symbol, enabled, priority) VALUES
    ('BTC/USD',  'CRYPTO', 'BTCUSD',  TRUE, 10),
    ('ETH/USD',  'CRYPTO', 'ETHUSD',  TRUE, 20),
    ('SOL/USD',  'CRYPTO', 'SOLUSD',  TRUE, 30),
    ('XRP/USD',  'CRYPTO', 'XRPUSD',  TRUE, 40),
    ('DOGE/USD', 'CRYPTO', 'DOGEUSD', TRUE, 50),
    ('LINK/USD', 'CRYPTO', 'LINKUSD', TRUE, 60),
    ('AVAX/USD', 'CRYPTO', 'AVAXUSD', TRUE, 70),
    ('LTC/USD',  'CRYPTO', 'LTCUSD',  TRUE, 80),
    ('BCH/USD',  'CRYPTO', 'BCHUSD',  TRUE, 90),
    ('ADA/USD',  'CRYPTO', 'ADAUSD',  TRUE, 100),
    ('DOT/USD',  'CRYPTO', 'DOTUSD',  TRUE, 110),
    ('UNI/USD',  'CRYPTO', 'UNIUSD',  TRUE, 120),
    ('AAVE/USD', 'CRYPTO', 'AAVEUSD', TRUE, 130),
    ('ATOM/USD', 'CRYPTO', 'ATOMUSD', TRUE, 140),
    ('NEAR/USD', 'CRYPTO', 'NEARUSD', TRUE, 150)
ON CONFLICT (market, symbol) DO NOTHING;

-- Seed equity watchlist (optional — feed not started by default)
INSERT INTO instruments (symbol, market, broker_symbol, enabled, priority) VALUES
    ('AAPL',  'EQUITY', 'AAPL',  FALSE, 10),
    ('MSFT',  'EQUITY', 'MSFT',  FALSE, 20),
    ('GOOGL', 'EQUITY', 'GOOGL', FALSE, 30),
    ('AMZN',  'EQUITY', 'AMZN',  FALSE, 40),
    ('NVDA',  'EQUITY', 'NVDA',  FALSE, 50),
    ('TSLA',  'EQUITY', 'TSLA',  FALSE, 60),
    ('META',  'EQUITY', 'META',  FALSE, 70),
    ('SPY',   'EQUITY', 'SPY',   FALSE, 80)
ON CONFLICT (market, symbol) DO NOTHING;

INSERT INTO instruments (symbol, market, broker_symbol, enabled, priority) VALUES
    ('GLD',  'COMMODITY', 'GLD',  FALSE, 10),
    ('USO',  'COMMODITY', 'USO',  FALSE, 20),
    ('SLV',  'COMMODITY', 'SLV',  FALSE, 30),
    ('UNG',  'COMMODITY', 'UNG',  FALSE, 40),
    ('WEAT', 'COMMODITY', 'WEAT', FALSE, 50)
ON CONFLICT (market, symbol) DO NOTHING;

INSERT INTO stat_arb_pairs (leg1, leg2, enabled, priority) VALUES
    ('BTC/USD',  'ETH/USD', TRUE, 10),
    ('ETH/USD',  'SOL/USD', TRUE, 20),
    ('BTC/USD',  'SOL/USD', TRUE, 30),
    ('DOGE/USD', 'XRP/USD', TRUE, 40),
    ('LINK/USD', 'ETH/USD', TRUE, 50)
ON CONFLICT (leg1, leg2) DO NOTHING;

