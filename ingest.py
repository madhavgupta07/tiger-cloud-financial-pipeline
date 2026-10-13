"""Real-time financial data pipeline: Twelve Data websocket -> Tiger Cloud.

Streams price events for the configured symbols and batches inserts into the
`crypto_ticks` hypertable. Configuration is read from environment variables
(see .env.example).
"""

import os
import signal
import sys
import time
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from twelvedata import TDClient

load_dotenv()


def build_connection():
    """Connect to Tiger Cloud via DATABASE_URL or standard PG* variables."""
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return psycopg2.connect(database_url, sslmode=os.getenv("PGSSLMODE", "require"))
    return psycopg2.connect(
        database=os.getenv("PGDATABASE", "tsdb"),
        host=os.getenv("PGHOST"),
        user=os.getenv("PGUSER", "tsdbadmin"),
        password=os.getenv("PGPASSWORD"),
        port=os.getenv("PGPORT", "5432"),
        sslmode=os.getenv("PGSSLMODE", "require"),
    )


class WebsocketPipeline:
    DB_TABLE = "crypto_ticks"
    DB_COLUMNS = ["time", "symbol", "price", "day_volume"]

    def __init__(self, conn, max_batch_size=100, flush_interval_seconds=10):
        self.conn = conn
        self.max_batch_size = max_batch_size
        self.flush_interval_seconds = flush_interval_seconds
        self.current_batch = []
        self.insert_counter = 0
        self.last_flush = time.monotonic()
        self._stopping = False

    def _insert_values(self, data):
        if not data or self.conn is None:
            return
        with self.conn.cursor() as cursor:
            sql = (
                f"INSERT INTO {self.DB_TABLE} ({','.join(self.DB_COLUMNS)}) "
                "VALUES %s;"
            )
            execute_values(cursor, sql, data)
        self.conn.commit()
        self.insert_counter += 1
        print(f"[ingest] batch #{self.insert_counter}: {len(data)} rows committed")

    def _flush(self):
        if self.current_batch:
            self._insert_values(self.current_batch)
            self.current_batch = []
        self.last_flush = time.monotonic()

    def _on_event(self, event):
        # Ignore websocket metadata (subscribe-status, heartbeat, ...).
        if event.get("event") != "price":
            return
        timestamp = datetime.fromtimestamp(event["timestamp"], tz=timezone.utc)
        self.current_batch.append(
            (timestamp, event["symbol"], event.get("price"), event.get("day_volume"))
        )
        print(f"[ingest] current batch size: {len(self.current_batch)}")
        if len(self.current_batch) >= self.max_batch_size:
            self._flush()

    def _handle_signal(self, *_args):
        print("\n[ingest] signal received, flushing remaining rows...")
        self._stopping = True

    def start(self, symbols, api_key):
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        td = TDClient(apikey=api_key)
        ws = td.websocket(on_event=self._on_event)
        ws.subscribe(symbols)
        ws.connect()
        print(f"[ingest] subscribed to: {', '.join(symbols)}")

        while not self._stopping:
            ws.heartbeat()
            if time.monotonic() - self.last_flush >= self.flush_interval_seconds:
                self._flush()
            time.sleep(1)

        self._flush()
        ws.disconnect()
        print("[ingest] stopped.")


def main():
    api_key = os.getenv("TWELVE_DATA_API_KEY")
    if not api_key:
        print("error: TWELVE_DATA_API_KEY is not set", file=sys.stderr)
        return 1

    symbols = [
        s.strip()
        for s in os.getenv("SYMBOLS", "BTC/USD,ETH/USD,MSFT,AAPL").split(",")
        if s.strip()
    ]

    conn = build_connection()
    pipeline = WebsocketPipeline(
        conn,
        max_batch_size=int(os.getenv("MAX_BATCH_SIZE", "100")),
        flush_interval_seconds=int(os.getenv("FLUSH_INTERVAL_SECONDS", "10")),
    )
    try:
        pipeline.start(symbols, api_key)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
