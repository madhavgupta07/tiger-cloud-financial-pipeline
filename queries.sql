-- Confirm rows are flowing in.
SELECT count(*) AS ticks, count(DISTINCT symbol) AS symbols
FROM crypto_ticks;

-- Latest ticks per symbol.
SELECT DISTINCT ON (symbol) symbol, "time", price, day_volume
FROM crypto_ticks
ORDER BY symbol, "time" DESC;

-- OHLCV candles for the past 14 days (the query Grafana visualizes).
SELECT *
FROM one_day_candle
WHERE symbol = 'BTC/USD' AND bucket >= NOW() - INTERVAL '14 days'
ORDER BY bucket;

-- Manually refresh the continuous aggregate (normally scheduled).
CALL refresh_continuous_aggregate('one_day_candle', NULL, NULL);
