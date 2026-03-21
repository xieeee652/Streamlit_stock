import re
import uuid
from collections import defaultdict
from datetime import datetime

import pandas as pd
import streamlit as st
import yfinance as yf
from streamlit_autorefresh import st_autorefresh

from db import delete_holding, find_or_create_user, init_db, load_holdings, upsert_holding

# -- Init DB (once per server session, not on every rerun) --------------------
@st.cache_resource
def _init_db():
    init_db()

_init_db()

st.set_page_config(
    page_title="Stock Portfolio Tracker",
    page_icon="📈",
    layout="wide",
)

# -- URL-based identity -------------------------------------------------------
# Each browser gets a UUID in the URL (?uid=...).
# First visit: generate UUID and inject it into the URL (triggers one rerun).
# Subsequent visits: read UUID from URL and load the user's portfolio.
params = st.query_params
browser_id = params.get("uid")

if not browser_id:
    st.query_params["uid"] = str(uuid.uuid4())
    st.stop()   # wait for the rerun with the new param

# -- Session state defaults ---------------------------------------------------
for key, default in {
    "user_id": None,
    "portfolio": {},
    "cached_news": {},
    "news_portfolio_key": "",
    "refresh_interval": 5,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

# Resolve user_id from browser_id once per session
if st.session_state.user_id is None:
    uid = find_or_create_user(browser_id)
    st.session_state.user_id = uid
    st.session_state.portfolio = load_holdings(uid)


# -- Helpers ------------------------------------------------------------------
def normalize_ticker(raw: str) -> str:
    """Append .TW suffix for numeric Taiwan stock/ETF codes (3-6 digits)."""
    s = raw.strip().upper()
    if re.search(r'\.[A-Z]{2,}$', s):
        return s
    if re.fullmatch(r'\d{3,6}', s):
        return s + ".TW"
    return s


# =============================================================================
#  MAIN APP
# =============================================================================

# -- Auto-refresh (price only) ------------------------------------------------
st_autorefresh(
    interval=st.session_state.refresh_interval * 1000,
    key="price_autorefresh",
)

# -- Sidebar ------------------------------------------------------------------
with st.sidebar:
    st.title("📋 My Holdings")

    with st.form("add_form", clear_on_submit=True):
        st.subheader("Add Holding")
        ticker_input = st.text_input(
            "Ticker Symbol",
            placeholder="US: AAPL   TW: 0050 or 2330",
        )
        st.caption("Taiwan stocks: enter number only, e.g. 0050, 2330, 2317")
        qty_input = st.number_input("Shares", min_value=0.01, step=1.0, value=1.0)
        price_input = st.number_input(
            "Avg. Buy Price (0 = skip P&L)",
            min_value=0.0,
            step=0.01,
            value=0.0,
        )
        add_btn = st.form_submit_button("➕ Add")

    if add_btn and ticker_input.strip():
        sym = normalize_ticker(ticker_input.strip())
        st.session_state.portfolio[sym] = {"quantity": qty_input, "avg_price": price_input}
        upsert_holding(st.session_state.user_id, sym, qty_input, price_input)
        st.success(f"Added {sym}")

    if st.session_state.portfolio:
        st.divider()
        st.subheader("Remove Holding")
        remove_sym = st.selectbox("Select ticker", list(st.session_state.portfolio))
        if st.button("🗑️ Remove"):
            del st.session_state.portfolio[remove_sym]
            delete_holding(st.session_state.user_id, remove_sym)
            st.rerun()

    st.divider()
    st.subheader("⏱️ Auto Refresh")
    st.session_state.refresh_interval = st.slider(
        "Price refresh interval (seconds)",
        min_value=1,
        max_value=60,
        value=st.session_state.refresh_interval,
        help="Minimum 1s — frequent requests may trigger Yahoo Finance rate limits",
    )
    st.divider()
    st.caption(f"🔑 Your ID: `{browser_id[:8]}…`")
    st.caption("Bookmark this page URL to keep your portfolio.")

# -- Main title ---------------------------------------------------------------
st.title("📈 Stock Portfolio Tracker")

if not st.session_state.portfolio:
    st.info("👈 Add holdings in the sidebar to view your portfolio summary and news.")
    st.markdown(
        """
        **Supported formats:**
        | Market | Example |
        |--------|---------|
        | US     | `AAPL`, `TSLA`, `NVDA` |
        | Taiwan | `0050`, `2330`, `2317` |
        | HK     | `0700.HK` |
        """
    )
    st.stop()

# -- Fetch prices (every refresh) ---------------------------------------------
rows = []
fetch_errors = []

# Invalidate news cache when portfolio changes
current_key = ",".join(sorted(st.session_state.portfolio.keys()))
if current_key != st.session_state.news_portfolio_key:
    st.session_state.cached_news = {}
    st.session_state.news_portfolio_key = current_key

with st.spinner("Fetching latest prices…"):
    tickers = list(st.session_state.portfolio.keys())
    # Batch-fetch all prices in one API call
    try:
        raw = yf.download(
            tickers,
            period="1d",
            interval="1m",
            progress=False,
            auto_adjust=True,
            group_by="ticker",
        )
        # Build a {sym: last_close} map from the batch result
        batch_prices: dict[str, float] = {}
        if len(tickers) == 1:
            sym = tickers[0]
            try:
                batch_prices[sym] = float(raw["Close"].dropna().iloc[-1])
            except Exception:
                pass
        else:
            for sym in tickers:
                try:
                    batch_prices[sym] = float(raw[sym]["Close"].dropna().iloc[-1])
                except Exception:
                    pass
    except Exception:
        batch_prices = {}

    for sym, holding in st.session_state.portfolio.items():
        price = batch_prices.get(sym, 0.0)
        currency = "N/A"
        if price == 0.0:
            # Fallback to fast_info for currency and price
            try:
                fi = yf.Ticker(sym).fast_info
                price = fi.last_price or 0.0
                currency = fi.currency or "N/A"
            except Exception as e:
                fetch_errors.append(f"{sym}: {e}")
        else:
            try:
                currency = yf.Ticker(sym).fast_info.currency or "N/A"
            except Exception:
                pass

        qty = holding["quantity"]
        avg_p = holding["avg_price"]
        mkt_val = price * qty
        cost = avg_p * qty if avg_p > 0 else None
        pnl = (mkt_val - cost) if cost else None
        pnl_pct = (pnl / cost * 100) if cost else None

        rows.append({
            "sym": sym, "qty": qty, "avg_p": avg_p,
            "price": price, "mkt_val": mkt_val, "currency": currency,
            "cost": cost, "pnl": pnl, "pnl_pct": pnl_pct,
        })

# -- Fetch news (cached, only refreshed on portfolio change) ------------------
if not st.session_state.cached_news and st.session_state.portfolio:
    with st.spinner("Fetching news…"):
        for sym in st.session_state.portfolio:
            try:
                items = yf.Ticker(sym).news
                if items:
                    st.session_state.cached_news[sym] = items[:5]
            except Exception:
                pass

for err in fetch_errors:
    st.warning(f"⚠️ Failed to fetch data: {err}")

# -- Portfolio summary --------------------------------------------------------
st.header("💼 Portfolio Overview")

by_currency_val = defaultdict(float)
by_currency_cost = defaultdict(float)
by_currency_pnl = defaultdict(float)

for r in rows:
    by_currency_val[r["currency"]] += r["mkt_val"]
    if r["cost"] is not None:
        by_currency_cost[r["currency"]] += r["cost"]
    if r["pnl"] is not None:
        by_currency_pnl[r["currency"]] += r["pnl"]

currencies = list(by_currency_val.keys())
metric_cols = st.columns(max(len(currencies) * 2, 1))

for i, ccy in enumerate(currencies):
    total_val = by_currency_val[ccy]
    total_pnl = by_currency_pnl.get(ccy)
    total_cost = by_currency_cost.get(ccy, 0)
    pnl_pct_str = (
        f"{total_pnl / total_cost * 100:+.2f}%"
        if total_pnl is not None and total_cost > 0
        else None
    )
    metric_cols[i * 2].metric(label=f"Total Value ({ccy})", value=f"{total_val:,.2f}")
    if total_pnl is not None:
        metric_cols[i * 2 + 1].metric(
            label=f"Total P&L ({ccy})",
            value=f"{total_pnl:+,.2f}",
            delta=pnl_pct_str,
        )

# -- Holdings table -----------------------------------------------------------
st.subheader("Holdings Detail")

table = []
for r in rows:
    table.append({
        "Ticker": r["sym"],
        "Currency": r["currency"],
        "Shares": r["qty"],
        "Avg. Buy Price": f'{r["avg_p"]:.2f}' if r["avg_p"] > 0 else "—",
        "Current Price": f'{r["price"]:.2f}',
        "Market Value": f'{r["mkt_val"]:,.2f}',
        "P&L": f'{r["pnl"]:+,.2f}' if r["pnl"] is not None else "—",
        "P&L %": f'{r["pnl_pct"]:+.2f}%' if r["pnl_pct"] is not None else "—",
    })

st.dataframe(pd.DataFrame(table), use_container_width=True, hide_index=True)
st.caption("Positive P&L % (green) = profit; Negative (red) = loss.")

col_time, col_btn = st.columns([3, 1])
col_time.caption(
    f"⏰ Last updated: {datetime.now().strftime('%H:%M:%S')}  "
    f"(auto-refresh every {st.session_state.refresh_interval}s)"
)
if col_btn.button("🔄 Refresh Now"):
    st.rerun()

# -- News ---------------------------------------------------------------------
st.header("📰 Related News")

if not st.session_state.cached_news:
    st.warning("No news found. Please check that the ticker symbols are correct.")
else:
    tabs = st.tabs(list(st.session_state.cached_news.keys()))
    for tab, sym in zip(tabs, st.session_state.cached_news.keys()):
        with tab:
            for article in st.session_state.cached_news[sym]:
                title = article.get("title") or "(no title)"
                link = article.get("link") or "#"
                publisher = article.get("publisher") or ""
                ts = article.get("providerPublishTime")
                dt_str = (
                    datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M") if ts else ""
                )
                st.markdown(f"#### [{title}]({link})")
                st.caption(f"{publisher}  {dt_str}")
                st.divider()