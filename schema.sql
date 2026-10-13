-- Global Hack Week: Hacktoberfest — Ingest real-time financial data into Tiger Cloud
-- Run this in the Tiger Cloud SQL editor (or psql) against your service.

-- 1. Relational table for asset metadata (standard PostgreSQL table).
CREATE TABLE IF NOT EXISTS crypto_assets (
    symbol TEXT UNIQUE,
    "name" TEXT
);

-- 2. Hypertable for the real-time tick data.
--    segmentby/orderby keep columnstore scans fast for per-symbol time queries.
CREATE TABLE IF NOT EXISTS crypto_ticks (
    "time"     TIMESTAMPTZ NOT NULL,
    symbol     TEXT NOT NULL,
    price      DOUBLE PRECISION,
    day_volume NUMERIC
) WITH (
    tsdb.hypertable,
    tsdb.segmentby = 'symbol',
    tsdb.orderby = 'time DESC'
);

-- 3. Continuous aggregate: one-day OHLCV candles per symbol.
CREATE MATERIALIZED VIEW IF NOT EXISTS one_day_candle
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', "time") AS bucket,
    symbol,
    FIRST(price, "time") AS "open",
    MAX(price)           AS high,
    MIN(price)           AS low,
    LAST(price, "time")  AS "close",
    LAST(day_volume, "time") AS day_volume
FROM crypto_ticks
GROUP BY bucket, symbol;

-- 4. Refresh policy: keep the last two days of candles current, daily.
SELECT add_continuous_aggregate_policy('one_day_candle',
    start_offset      => INTERVAL '3 days',
    end_offset        => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists     => TRUE);
