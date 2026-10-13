# Real-time financial data pipeline → Tiger Cloud

A data pipeline that streams real-time market data from the [Twelve Data](https://twelvedata.com)
websocket API into [Tiger Cloud](https://www.tigerdata.com) (TimescaleDB) using the
official [ingest real-time financial data](https://www.tigerdata.com/docs/build/examples/ingest-real-time-financial-data)
tutorial approach.

Price events are batched in memory and inserted into a `crypto_ticks` hypertable; a
continuous aggregate rolls the raw ticks up into one-day OHLCV candles that you can chart
in Grafana.

## Architecture

```
Twelve Data websocket ──► ingest.py (on_event, in-memory batch)
                              │  execute_values() every N rows / N seconds
                              ▼
                    Tiger Cloud (TimescaleDB)
                    ├── crypto_ticks   (hypertable: time, symbol, price, day_volume)
                    ├── crypto_assets  (relational table)
                    └── one_day_candle (continuous aggregate, OHLCV)
                              │
                              ▼
                            Grafana (candlestick chart)
```

## Prerequisites

- A Tiger Cloud service (free tier works). Get the connection URI from the console.
- A Twelve Data API key (free tier works): https://twelvedata.com/account/api-keys
- Python 3.9+.
- Optional: Grafana (self-managed or Grafana Cloud) for the candlestick chart.

## Setup

```bash
python -m venv env
source env/bin/activate        # Windows: env\Scripts\activate
pip install -r requirements.txt
cp .env.example .env           # Windows: copy .env.example .env
# edit .env with your DATABASE_URL and TWELVE_DATA_API_KEY
```

Create the schema in your Tiger Cloud service. Easiest (no `psql` needed):

```bash
python setup_db.py
```

Or paste `schema.sql` into the Tiger Cloud SQL editor. If you do have `psql`:

```bash
psql "postgres://tsdbadmin:PASSWORD@HOST:PORT/tsdb?sslmode=require" -f schema.sql
```

## Run

```bash
python ingest.py
```

You should see the subscription status, then a growing batch size and periodic
`batch #N: 100 rows committed` lines. Press Ctrl+C to flush and exit cleanly.

Verify data landed (see `queries.sql`):

```sql
SELECT count(*) FROM crypto_ticks;
SELECT * FROM one_day_candle WHERE symbol = 'BTC/USD'
  AND bucket >= NOW() - INTERVAL '14 days' ORDER BY bucket;
```

## Grafana candlestick chart (optional)

1. Add a PostgreSQL data source: host `<host>:<port>`, your database/user/password,
   TLS/SSL mode `require`, and enable the `TimescaleDB` PostgreSQL option.
2. New dashboard → Add visualization → `Candlestick`.
3. Paste the OHLCV query from `queries.sql` (the `one_day_candle` SELECT).

## Submitting the challenge

The challenge asks for a screenshot of and/or a link to your data pipeline. Good evidence:

- A screenshot of `ingest.py` running with batch commits printing live.
- A screenshot of the row count / latest ticks in the Tiger Cloud SQL editor.
- A screenshot of the Grafana candlestick chart (optional but strong).
- A link to this repository.

## Notes

- `datetime.fromtimestamp(ts, tz=timezone.utc)` is used instead of the deprecated
  `utcfromtimestamp`.
- Increase `MAX_BATCH_SIZE` (100 → 1000 → 10000) to compare ingest throughput.
- Run multiple instances with different `SYMBOLS` to separate stocks and crypto streams.
