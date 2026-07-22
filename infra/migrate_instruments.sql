-- Idempotent migration for existing Timescale volumes (init-db.sql only runs once).
-- Usage: psql "$DATABASE_URL" -f infra/migrate_instruments.sql

CREATE TABLE IF NOT EXISTS instruments (
    id              SERIAL PRIMARY KEY,
    symbol          TEXT NOT NULL,
    market          TEXT NOT NULL,
    broker_symbol   TEXT NOT NULL,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    priority        INT NOT NULL DEFAULT 100,
    notes           TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (market, symbol)
);
CREATE INDEX IF NOT EXISTS instruments_enabled_idx
    ON instruments (market, enabled, priority);

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

INSERT INTO instruments (symbol, market, broker_symbol, enabled, priority) VALUES
    ('EUR/USD', 'FOREX', 'EUR_USD', TRUE, 10),
    ('GBP/USD', 'FOREX', 'GBP_USD', TRUE, 20),
    ('USD/JPY', 'FOREX', 'USD_JPY', TRUE, 30),
    ('AUD/USD', 'FOREX', 'AUD_USD', TRUE, 40),
    ('USD/CAD', 'FOREX', 'USD_CAD', TRUE, 50),
    ('USD/CHF', 'FOREX', 'USD_CHF', TRUE, 60),
    ('EUR/GBP', 'FOREX', 'EUR_GBP', TRUE, 70),
    ('EUR/JPY', 'FOREX', 'EUR_JPY', TRUE, 80),
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

INSERT INTO stat_arb_pairs (leg1, leg2, enabled, priority) VALUES
    ('BTC/USD',  'ETH/USD', TRUE, 10),
    ('ETH/USD',  'SOL/USD', TRUE, 20),
    ('BTC/USD',  'SOL/USD', TRUE, 30),
    ('DOGE/USD', 'XRP/USD', TRUE, 40),
    ('LINK/USD', 'ETH/USD', TRUE, 50)
ON CONFLICT (leg1, leg2) DO NOTHING;
