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
CREATE INDEX ON ticks (symbol, time DESC);

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
