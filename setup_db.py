"""Apply schema.sql to your Tiger Cloud service (no psql required).

Usage:
    python setup_db.py
"""

import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def build_connection():
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


def statements(sql):
    """Split a .sql file into individual statements.

    TimescaleDB's continuous-aggregate creation cannot run inside a transaction
    block, so each statement is executed on its own (with autocommit).
    """
    for raw in sql.split(";"):
        chunk = raw.strip()
        if not chunk:
            continue
        code_lines = [ln for ln in chunk.splitlines() if not ln.strip().startswith("--")]
        if "".join(code_lines).strip():
            yield chunk


def main():
    schema = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
    conn = build_connection()
    conn.autocommit = True
    try:
        with conn.cursor() as cursor:
            for statement in statements(schema):
                cursor.execute(statement)
        print("[setup] schema applied: crypto_ticks, crypto_assets, one_day_candle")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
