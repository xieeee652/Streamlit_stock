"""
Database layer - PostgreSQL (Supabase) backed portfolio persistence.
Connection URL is read from st.secrets["DATABASE_URL"] (Streamlit Cloud)
or the DATABASE_URL environment variable (local dev).
"""

import os
from contextlib import contextmanager
from pathlib import Path

import psycopg2
import psycopg2.pool


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


_pool = None


def _get_pool():
    """Return a lazily-initialized connection pool (singleton)."""
    global _pool
    if _pool is None:
        params = _db_params()
        _pool = psycopg2.pool.ThreadedConnectionPool(1, 5, **params)
    return _pool


@contextmanager
def _conn():
    pool = _get_pool()
    con = pool.getconn()
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        pool.putconn(con)


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
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS transactions (
                    id         SERIAL PRIMARY KEY,
                    user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    ticker     TEXT    NOT NULL,
                    trade_date DATE    NOT NULL,
                    quantity   REAL    NOT NULL,
                    price      REAL    NOT NULL,
                    trade_type TEXT    NOT NULL DEFAULT 'buy'
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS price_alerts (
                    id          SERIAL PRIMARY KEY,
                    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    ticker      TEXT    NOT NULL,
                    target_price REAL,
                    stop_price   REAL,
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
                "DELETE FROM transactions WHERE user_id = %s AND ticker = %s",
                (user_id, ticker),
            )
            cur.execute(
                "DELETE FROM holdings WHERE user_id = %s AND ticker = %s",
                (user_id, ticker),
            )


# -- Transactions -------------------------------------------------------------

def _sync_holding_from_transactions(cur, user_id: int, ticker: str) -> None:
    """Recalculate holding qty/avg_price from buy transactions; noop if no buys."""
    cur.execute(
        "SELECT quantity, price, trade_type FROM transactions WHERE user_id=%s AND ticker=%s",
        (user_id, ticker),
    )
    rows = cur.fetchall()
    buy_rows  = [(q, p) for q, p, tt in rows if tt == "buy"]
    sell_rows = [(q, p) for q, p, tt in rows if tt == "sell"]

    # If no buys, leave the holding untouched (managed via "Add Holding" form)
    if not buy_rows:
        return

    buy_qty  = sum(q for q, p in buy_rows)
    buy_cost = sum(q * p for q, p in buy_rows)
    sell_qty = sum(q for q, p in sell_rows)
    net_qty  = buy_qty - sell_qty
    avg_price = buy_cost / buy_qty if buy_qty > 0 else 0.0

    if net_qty <= 0:
        cur.execute(
            "DELETE FROM holdings WHERE user_id=%s AND ticker=%s",
            (user_id, ticker),
        )
        return

    cur.execute(
        """
        INSERT INTO holdings (user_id, ticker, quantity, avg_price)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (user_id, ticker) DO UPDATE SET
            quantity  = EXCLUDED.quantity,
            avg_price = EXCLUDED.avg_price
        """,
        (user_id, ticker, net_qty, avg_price),
    )


def add_transaction(
    user_id: int, ticker: str, trade_date, quantity: float,
    price: float, trade_type: str,
) -> None:
    with _conn() as con:
        with con.cursor() as cur:
            cur.execute(
                """
                INSERT INTO transactions (user_id, ticker, trade_date, quantity, price, trade_type)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (user_id, ticker, str(trade_date), quantity, price, trade_type),
            )
            _sync_holding_from_transactions(cur, user_id, ticker)


def load_transactions(user_id: int, ticker: str = None) -> list:
    """Return list of (id, ticker, trade_date, quantity, price, trade_type)."""
    with _conn() as con:
        with con.cursor() as cur:
            if ticker:
                cur.execute(
                    """SELECT id, ticker, trade_date, quantity, price, trade_type
                       FROM transactions WHERE user_id=%s AND ticker=%s
                       ORDER BY trade_date DESC, id DESC""",
                    (user_id, ticker),
                )
            else:
                cur.execute(
                    """SELECT id, ticker, trade_date, quantity, price, trade_type
                       FROM transactions WHERE user_id=%s
                       ORDER BY trade_date DESC, id DESC""",
                    (user_id,),
                )
            return cur.fetchall()


def delete_transaction(txn_id: int, user_id: int):
    """Delete a transaction, resync its holding. Returns affected ticker or None."""
    with _conn() as con:
        with con.cursor() as cur:
            cur.execute(
                "SELECT ticker FROM transactions WHERE id=%s AND user_id=%s",
                (txn_id, user_id),
            )
            row = cur.fetchone()
            if not row:
                return None
            ticker = row[0]
            cur.execute(
                "DELETE FROM transactions WHERE id=%s AND user_id=%s",
                (txn_id, user_id),
            )
            _sync_holding_from_transactions(cur, user_id, ticker)
            return ticker


# -- Price Alerts --------------------------------------------------------------

def load_price_alerts(user_id: int) -> dict:
    """Return {ticker: {target_price, stop_price}}."""
    with _conn() as con:
        with con.cursor() as cur:
            cur.execute(
                "SELECT ticker, target_price, stop_price FROM price_alerts WHERE user_id = %s",
                (user_id,),
            )
            rows = cur.fetchall()
    return {r[0]: {"target_price": r[1], "stop_price": r[2]} for r in rows}


def upsert_price_alert(user_id: int, ticker: str, target_price: float = None, stop_price: float = None) -> None:
    with _conn() as con:
        with con.cursor() as cur:
            cur.execute(
                """
                INSERT INTO price_alerts (user_id, ticker, target_price, stop_price)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id, ticker) DO UPDATE SET
                    target_price = EXCLUDED.target_price,
                    stop_price   = EXCLUDED.stop_price
                """,
                (user_id, ticker, target_price, stop_price),
            )


def delete_price_alert(user_id: int, ticker: str) -> None:
    with _conn() as con:
        with con.cursor() as cur:
            cur.execute(
                "DELETE FROM price_alerts WHERE user_id = %s AND ticker = %s",
                (user_id, ticker),
            )