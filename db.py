"""
Database layer - PostgreSQL (Supabase) backed portfolio persistence.
Connection URL is read from st.secrets["DATABASE_URL"] (Streamlit Cloud)
or the DATABASE_URL environment variable (local dev).
"""

import os
from contextlib import contextmanager
from pathlib import Path

import psycopg2


def _read_secrets_toml() -> dict:
    """Fallback: read .streamlit/secrets.toml directly when outside Streamlit."""
    p = Path(__file__).parent / ".streamlit" / "secrets.toml"
    if not p.exists():
        return {}
    try:
        import tomllib  # Python 3.11+
    except ImportError:
        try:
            import tomli as tomllib
        except ImportError:
            return {}
    # Read as bytes and strip UTF-8 BOM if present
    raw = p.read_bytes()
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    return tomllib.loads(raw.decode("utf-8"))


def _db_params() -> dict:
    """Return psycopg2 connection kwargs from secrets or environment."""
    # 1. Try st.secrets (inside a running Streamlit app)
    try:
        import streamlit as st
        sec = st.secrets
        if "db" in sec:
            d = sec["db"]
            return dict(host=d["host"], port=int(d.get("port", 5432)),
                        dbname=d["dbname"], user=d["user"],
                        password=d["password"], sslmode="require")
        if "DATABASE_URL" in sec:
            return {"dsn": sec["DATABASE_URL"]}
    except Exception:
        pass

    # 2. Try reading secrets.toml directly (outside Streamlit, e.g. CLI)
    sec = _read_secrets_toml()
    if "db" in sec:
        d = sec["db"]
        return dict(host=d["host"], port=int(d.get("port", 5432)),
                    dbname=d["dbname"], user=d["user"],
                    password=d["password"], sslmode="require")
    if "DATABASE_URL" in sec:
        return {"dsn": sec["DATABASE_URL"]}

    # 3. Fall back to environment variable
    url = os.environ.get("DATABASE_URL", "")
    if url:
        return {"dsn": url}

    raise RuntimeError(
        "No database configuration found. "
        "Add [db] section to .streamlit/secrets.toml or set DATABASE_URL."
    )


@contextmanager
def _conn():
    params = _db_params()
    con = psycopg2.connect(**params)
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def init_db() -> None:
    with _conn() as con:
        with con.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id         SERIAL PRIMARY KEY,
                    browser_id TEXT UNIQUE NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS holdings (
                    id        SERIAL PRIMARY KEY,
                    user_id   INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    ticker    TEXT    NOT NULL,
                    quantity  REAL    NOT NULL,
                    avg_price REAL    NOT NULL DEFAULT 0,
                    UNIQUE (user_id, ticker)
                )
                """
            )


# -- User ---------------------------------------------------------------------

def find_or_create_user(browser_id: str) -> int:
    """Return user_id for the given browser_id, creating a row if needed."""
    with _conn() as con:
        with con.cursor() as cur:
            cur.execute("SELECT id FROM users WHERE browser_id = %s", (browser_id,))
            row = cur.fetchone()
            if row:
                return row[0]
            cur.execute(
                "INSERT INTO users (browser_id) VALUES (%s) RETURNING id",
                (browser_id,),
            )
            return cur.fetchone()[0]


# -- Holdings CRUD ------------------------------------------------------------

def load_holdings(user_id: int) -> dict:
    """Return { ticker: {quantity, avg_price} }."""
    with _conn() as con:
        with con.cursor() as cur:
            cur.execute(
                "SELECT ticker, quantity, avg_price FROM holdings WHERE user_id = %s",
                (user_id,),
            )
            rows = cur.fetchall()
    return {r[0]: {"quantity": r[1], "avg_price": r[2]} for r in rows}


def upsert_holding(user_id: int, ticker: str, quantity: float, avg_price: float) -> None:
    with _conn() as con:
        with con.cursor() as cur:
            cur.execute(
                """
                INSERT INTO holdings (user_id, ticker, quantity, avg_price)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id, ticker) DO UPDATE SET
                    quantity  = EXCLUDED.quantity,
                    avg_price = EXCLUDED.avg_price
                """,
                (user_id, ticker, quantity, avg_price),
            )


def delete_holding(user_id: int, ticker: str) -> None:
    with _conn() as con:
        with con.cursor() as cur:
            cur.execute(
                "DELETE FROM holdings WHERE user_id = %s AND ticker = %s",
                (user_id, ticker),
            )