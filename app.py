import re
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from html import escape as html_escape
from typing import Optional
import json

import pandas as pd
import plotly.graph_objects as go
import streamlit.components.v1 as components
import streamlit as st
import yfinance as yf
from streamlit_autorefresh import st_autorefresh

from db import (
    add_transaction, delete_holding, delete_transaction,
    delete_price_alert, find_or_create_user, init_db, load_holdings,
    load_price_alerts, load_transactions, upsert_holding, upsert_price_alert,
)

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
    "refresh_interval": 30,
    "lang": "zh",
    "last_prices": {},
    "last_currencies": {},
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

# Resolve user_id from browser_id once per session
if st.session_state.user_id is None:
    uid = find_or_create_user(browser_id)
    st.session_state.user_id = uid
    st.session_state.portfolio = load_holdings(uid)


# -- i18n ---------------------------------------------------------------------
_LANG: dict = {
    "zh": {
        "sidebar_title": "📋 我的持股",
        "add_form_title": "新增持股",
        "ticker_label": "股票代號",
        "ticker_placeholder": "美股: AAPL   台股: 0050 或 2330",
        "ticker_caption": "台股只需輸入數字，如 0050、2330、2317",
        "shares_label": "股數",
        "avg_price_label": "平均買入價（0 = 不計算損益）",
        "add_btn": "➕ 新增",
        "added_msg": "已新增 {}",
        "edit_title": "✏️ 編輯持股",
        "edit_select": "選擇要編輯的股票",
        "avg_price_edit_label": "平均買入價",
        "save_btn": "💾 儲存",
        "updated_msg": "{} 已更新",
        "remove_title": "🗑️ 移除持股",
        "remove_select": "選擇股票",
        "remove_btn": "🗑️ 移除",
        "refresh_title": "⏱️ 自動更新",
        "refresh_label": "股價更新間隔（秒）",
        "refresh_help": "最小 5 秒，頻繁請求可能觸發 API 限速",
        "id_caption": "請將此頁面網址加入書籤以保存你的持倉",
        "lang_toggle": "🌐 English",
        "app_title": "📈 股票持倉追蹤",
        "empty_info": "👈 請在側邊欄新增持股，以檢視持倉概覽與新聞",
        "supported_formats": "**支援格式：**\n| 市場 | 範例 |\n|------|------|\n| 美股 | `AAPL`, `TSLA`, `NVDA` |\n| 台股 | `0050`, `2330`, `2317` |\n| 港股 | `0700.HK` |",
        "fetching_prices": "正在取得最新股價…",
        "market_tw_only": "🇹🇼 台股交易時段，略過美股（使用快取）",
        "market_us_only": "🇺🇸 美股交易時段，略過台股（使用快取）",
        "market_both": "🌐 更新所有持股",
        "fetching_news": "正在取得新聞…",
        "fetch_failed": "⚠️ 無法取得資料：{}",
        "sec_overview": "💼 持倉總覽",
        "sec_detail": "📊 持倉明細",
        "sec_chart": "📈 股價走勢",
        "sec_pie": "🥧 持倉比重",
        "sec_news": "📰 相關新聞",
        "total_value": "總市值 ({})",
        "total_pnl": "總損益 ({})",
        "col_ticker": "股票代號",
        "col_shares": "股數",
        "col_avg_price": "平均買入價",
        "col_current_price": "現價",
        "col_mkt_val": "市值",
        "col_pnl": "損益",
        "col_pnl_pct": "損益%",
        "col_52w_high": "52週高",
        "col_52w_low": "52週低",
        "col_52w_range": "位置",
        "tw_stocks": "🇹🇼 台股（TWD）",
        "us_stocks": "🇺🇸 美股（USD）",
        "hk_stocks": "🇭🇰 港股（HKD）",
        "no_tw": "尚無台股持倉",
        "no_us": "尚無美股持倉",
        "no_hk": "尚無港股持倉",
        "chart_sym_label": "選擇股票",
        "chart_period_label": "期間",
        "chart_period_opts": ["1週", "1個月", "3個月", "6個月", "1年", "2年"],
        "chart_indicators_label": "技術指標",
        "chart_cost_label": "成本 {:.2f}",
        "chart_no_data": "無法取得 {} 的歷史資料",
        "bb_upper": "BB上軌",
        "bb_lower": "BB下軌",
        "bb_mid": "BB中軌",
        "pie_tw": "🇹🇼 台股配置",
        "pie_us": "🇺🇸 美股配置",
        "pie_hk": "🇭🇰 港股配置",
        "news_tw": "**🇹🇼 台股新聞**",
        "news_us": "**🇺🇸 美股新聞**",
        "news_hk": "**🇭🇰 港股新聞**",
        "no_news_tab": "尚無相關新聞",
        "news_not_found": "找不到新聞，請確認股票代號是否正確",
        "footer_time": "⏰ 最後更新：{}  （每 {}s 自動更新）",
        "refresh_now": "🔄 立即更新",
        # Feature 3: FX unified total
        "fx_label": "💱 換算基準幣別",
        "fx_unified_total": "統一總資產 ({})",
        "fx_unified_pnl": "統一總損益 ({})",
        "fx_rate_note": "匯率：1 USD ≈ {:.2f} TWD",
        "fx_unavailable": "匯率暫時無法取得，無法顯示統一換算",
        # Feature 1: Portfolio performance chart
        "sec_perf": "📊 投組績效走勢",
        "perf_period_label": "期間",
        "perf_period_opts": ["1個月", "3個月", "6個月", "1年"],
        "perf_period_vals": ["1mo", "3mo", "6mo", "1y"],
        "perf_tw_label": "台股市值 (TWD)",
        "perf_us_label": "美股市值 (USD)",
        "perf_hk_label": "港股市值 (HKD)",
        "perf_cost": "成本",
        "perf_fetching": "載入投組走勢…",
        "perf_no_data": "無法取得投組歷史資料",
        # Feature 2: Transaction log
        "trans_section": "📝 記錄交易",
        "trans_add_title": "新增一筆交易",
        "trans_ticker_label": "股票代號",
        "trans_type_label": "買/賣",
        "trans_buy": "買入",
        "trans_sell": "賣出",
        "trans_date_label": "交易日期",
        "trans_qty_label": "股數",
        "trans_price_label": "成交價",
        "trans_submit_btn": "✅ 確認新增",
        "trans_added": "已記錄：{} {} {}股 @ {:.2f}",
        "trans_error_sell": "⚠️ 賣出數量超過現有持股，請確認",
        "trans_history": "📝 交易紀錄",
        "trans_col_date": "日期", "trans_col_ticker": "代號",
        "trans_col_type": "類型", "trans_col_qty": "股數",
        "trans_col_price": "成交價",
        "trans_buy_label": "買", "trans_sell_label": "賣",
        "trans_deleted": "交易已刪除",
        "trans_no_records": "尚無交易紀錄",
        "trans_delete_help": "刪除此筆交易",
        # Feature 7: Sector distribution
        "sec_sector": "🏭 類股/板塊分佈",
        "sector_tw": "🇹🇼 台股板塊分佈",
        "sector_us": "🇺🇸 美股板塊分佈",
        "sector_hk": "🇭🇰 港股板塊分佈",
        "sector_unknown": "未知",
        "sector_fetching": "載入板塊資料…",
        "sector_label": "產業",
        # Feature 8: Dividend records
        "sec_dividend": "💰 股息紀錄",
        "div_col_sector": "產業別",
        "div_col_yield": "股息殖利率",
        "div_col_annual_rate": "每股年息",
        "div_col_annual_income": "預期年化股息收入",
        "div_tw_total": "台股合計預期年化股息 (TWD)",
        "div_us_total": "美股合計預期年化股息 (USD)",
        "div_hk_total": "港股合計預期年化股息 (HKD)",
        # Feature 5: Price alerts
        "alert_title": "🎯 目標價 / 停損價",
        "alert_select": "選擇股票",
        "alert_target_label": "目標出價（0 = 不設定）",
        "alert_stop_label": "停損價（0 = 不設定）",
        "alert_save_btn": "📌 設定",
        "alert_saved": "已設定 {} 的提醒",
        "alert_clear_btn": "❌ 清除提醒",
        "alert_cleared": "已清除 {} 的提醒",
        "alert_target_hit": "🚀 {sym} 已達目標價！現價 {price:.2f} ≥ 目標 {target:.2f}",
        "alert_stop_hit": "⚠️ {sym} 已觸停損！現價 {price:.2f} ≤ 停損 {stop:.2f}",
        "alert_current": "目前設定",
        "alert_target_short": "目標",
        "alert_stop_short": "停損",
    },
    "en": {
        "sidebar_title": "📋 My Holdings",
        "add_form_title": "Add Holding",
        "ticker_label": "Ticker Symbol",
        "ticker_placeholder": "US: AAPL   TW: 0050 or 2330",
        "ticker_caption": "Taiwan stocks: enter number only, e.g. 0050, 2330, 2317",
        "shares_label": "Shares",
        "avg_price_label": "Avg. Buy Price (0 = skip P&L)",
        "add_btn": "➕ Add",
        "added_msg": "Added {}",
        "edit_title": "✏️ Edit Holding",
        "edit_select": "Select ticker to edit",
        "avg_price_edit_label": "Avg. Buy Price",
        "save_btn": "💾 Save",
        "updated_msg": "{} updated",
        "remove_title": "🗑️ Remove Holding",
        "remove_select": "Select ticker",
        "remove_btn": "🗑️ Remove",
        "refresh_title": "⏱️ Auto Refresh",
        "refresh_label": "Price refresh interval (seconds)",
        "refresh_help": "Minimum 5s — frequent requests may trigger API rate limits",
        "id_caption": "Bookmark this page URL to keep your portfolio.",
        "lang_toggle": "🌐 中文",
        "app_title": "📈 Stock Portfolio Tracker",
        "empty_info": "👈 Add holdings in the sidebar to view your portfolio summary and news.",
        "supported_formats": "**Supported formats:**\n| Market | Example |\n|--------|---------|\n| US     | `AAPL`, `TSLA`, `NVDA` |\n| Taiwan | `0050`, `2330`, `2317` |\n| HK     | `0700.HK` |",
        "fetching_prices": "Fetching latest prices…",
        "market_tw_only": "🇹🇼 TW trading session — US prices from cache",
        "market_us_only": "🇺🇸 US trading session — TW prices from cache",
        "market_both": "🌐 Updating all holdings",
        "fetching_news": "Fetching news…",
        "fetch_failed": "⚠️ Failed to fetch data: {}",
        "sec_overview": "💼 Portfolio Overview",
        "sec_detail": "📊 Holdings Detail",
        "sec_chart": "📈 Stock Price Chart",
        "sec_pie": "🥧 Portfolio Allocation",
        "sec_news": "📰 Related News",
        "total_value": "Total Value ({})",
        "total_pnl": "Total P&L ({})",
        "col_ticker": "Ticker",
        "col_shares": "Shares",
        "col_avg_price": "Avg. Buy Price",
        "col_current_price": "Current Price",
        "col_mkt_val": "Market Value",
        "col_pnl": "P&L",
        "col_pnl_pct": "P&L %",
        "col_52w_high": "52W High",
        "col_52w_low": "52W Low",
        "col_52w_range": "Range",
        "tw_stocks": "🇹🇼 TW Stocks (TWD)",
        "us_stocks": "🇺🇸 US Stocks (USD)",
        "hk_stocks": "🇭🇰 HK Stocks (HKD)",
        "no_tw": "No TW holdings",
        "no_us": "No US holdings",
        "no_hk": "No HK holdings",
        "chart_sym_label": "Select Stock",
        "chart_period_label": "Period",
        "chart_period_opts": ["1W", "1M", "3M", "6M", "1Y", "2Y"],
        "chart_indicators_label": "Indicators",
        "chart_cost_label": "Cost {:.2f}",
        "chart_no_data": "No historical data for {}",
        "bb_upper": "BB Upper",
        "bb_lower": "BB Lower",
        "bb_mid": "BB Mid",
        "pie_tw": "🇹🇼 TW Allocation",
        "pie_us": "🇺🇸 US Allocation",
        "pie_hk": "🇭🇰 HK Allocation",
        "news_tw": "**🇹🇼 TW News**",
        "news_us": "**🇺🇸 US News**",
        "news_hk": "**🇭🇰 HK News**",
        "no_news_tab": "No news available",
        "news_not_found": "No news found. Please check that the ticker symbols are correct.",
        "footer_time": "⏰ Last updated: {}  (auto-refresh every {}s)",
        "refresh_now": "🔄 Refresh Now",
        # Feature 3: FX unified total
        "fx_label": "💱 Base Currency",
        "fx_unified_total": "Unified Total ({})",
        "fx_unified_pnl": "Unified P&L ({})",
        "fx_rate_note": "Rate: 1 USD ≈ {:.2f} TWD",
        "fx_unavailable": "FX rate unavailable — unified total cannot be shown",
        # Feature 1: Portfolio performance chart
        "sec_perf": "📊 Portfolio Performance",
        "perf_period_label": "Period",
        "perf_period_opts": ["1M", "3M", "6M", "1Y"],
        "perf_period_vals": ["1mo", "3mo", "6mo", "1y"],
        "perf_tw_label": "TW Value (TWD)",
        "perf_us_label": "US Value (USD)",
        "perf_hk_label": "HK Value (HKD)",
        "perf_cost": "Cost",
        "perf_fetching": "Loading portfolio performance…",
        "perf_no_data": "Unable to fetch portfolio history",
        # Feature 2: Transaction log
        "trans_section": "📝 Log a Trade",
        "trans_add_title": "New Transaction",
        "trans_ticker_label": "Ticker Symbol",
        "trans_type_label": "Type",
        "trans_buy": "Buy",
        "trans_sell": "Sell",
        "trans_date_label": "Trade Date",
        "trans_qty_label": "Shares",
        "trans_price_label": "Price",
        "trans_submit_btn": "✅ Submit",
        "trans_added": "Logged: {} {} {} shares @ {:.2f}",
        "trans_error_sell": "⚠️ Sell quantity exceeds current holdings",
        "trans_history": "📝 Transaction History",
        "trans_col_date": "Date", "trans_col_ticker": "Ticker",
        "trans_col_type": "Type", "trans_col_qty": "Shares",
        "trans_col_price": "Price",
        "trans_buy_label": "Buy", "trans_sell_label": "Sell",
        "trans_deleted": "Transaction deleted",
        "trans_no_records": "No transactions yet",
        "trans_delete_help": "Delete this transaction",
        # Feature 7: Sector distribution
        "sec_sector": "🏭 Sector Distribution",
        "sector_tw": "🇹🇼 TW Sector Breakdown",
        "sector_us": "🇺🇸 US Sector Breakdown",
        "sector_hk": "🇭🇰 HK Sector Breakdown",
        "sector_unknown": "Unknown",
        "sector_fetching": "Loading sector data…",
        "sector_label": "Sectors",
        # Feature 8: Dividend records
        "sec_dividend": "💰 Dividend Records",
        "div_col_sector": "Sector",
        "div_col_yield": "Div. Yield",
        "div_col_annual_rate": "Annual Div./Share",
        "div_col_annual_income": "Expected Annual Div.",
        "div_tw_total": "TW Total Expected Annual Dividend (TWD)",
        "div_us_total": "US Total Expected Annual Dividend (USD)",
        "div_hk_total": "HK Total Expected Annual Dividend (HKD)",
        # Feature 5: Price alerts
        "alert_title": "🎯 Target / Stop-Loss",
        "alert_select": "Select ticker",
        "alert_target_label": "Target price (0 = not set)",
        "alert_stop_label": "Stop-loss price (0 = not set)",
        "alert_save_btn": "📌 Set Alert",
        "alert_saved": "Alert set for {}",
        "alert_clear_btn": "❌ Clear Alert",
        "alert_cleared": "Alert cleared for {}",
        "alert_target_hit": "🚀 {sym} hit target! Price {price:.2f} ≥ Target {target:.2f}",
        "alert_stop_hit": "⚠️ {sym} hit stop-loss! Price {price:.2f} ≤ Stop {stop:.2f}",
        "alert_current": "Current alerts",
        "alert_target_short": "Target",
        "alert_stop_short": "Stop",
    },
}

def t(key: str) -> str:
    """Return translated string for current language."""
    return _LANG[st.session_state.get("lang", "zh")][key]


# -- Helpers ------------------------------------------------------------------
def normalize_ticker(raw: str) -> str:
    """Append .TW suffix for numeric Taiwan stock/ETF codes (3-6 digits)."""
    s = raw.strip().upper()
    if re.search(r'\.[A-Z]{2,}$', s):
        return s
    if re.fullmatch(r'\d{3,6}', s):
        return s + ".TW"
    return s


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_fx_rate(pair: str) -> Optional[float]:
    """Fetch the latest price for a forex pair (e.g. 'USDTWD=X')."""
    try:
        data = yf.download(pair, period="5d", interval="1d", progress=False, auto_adjust=True)
        if not data.empty:
            val = data["Close"].dropna().iloc[-1]
            # squeeze Series to scalar (avoids FutureWarning on newer pandas)
            if hasattr(val, "iloc"):
                val = val.iloc[0]
            return float(val)
        return float(yf.Ticker(pair).fast_info.last_price or 0) or None
    except Exception:
        return None


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_portfolio_history(holdings_json: str, period_str: str) -> dict:
    """
    Fetch portfolio value history grouped by currency.
    holdings_json: JSON list of {sym, qty, cost, currency}
    Returns {currency: {dates, values, total_cost}}
    """
    holdings = json.loads(holdings_json)
    if not holdings:
        return {}

    qty_map  = {h["sym"]: h["qty"]           for h in holdings}
    cost_map = {h["sym"]: h.get("cost") or 0 for h in holdings}
    ccy_map  = {h["sym"]: h["currency"]       for h in holdings}

    # Download each ticker individually (most reliable across yfinance versions)
    close_series: dict = {}
    for sym in qty_map:
        try:
            df = yf.download(sym, period=period_str, interval="1d",
                             progress=False, auto_adjust=True)
            if df.empty:
                continue
            col = df["Close"].squeeze()
            if isinstance(col, pd.DataFrame):
                col = col.iloc[:, 0]
            col = col.dropna()
            if not col.empty:
                close_series[sym] = col
        except Exception:
            continue

    if not close_series:
        return {}

    # Align all series to a common date index
    all_dates = sorted(set().union(*[s.index for s in close_series.values()]))
    if not all_dates:
        return {}
    idx = pd.DatetimeIndex(all_dates)

    ccy_groups: dict = {}
    for sym in close_series:
        ccy_groups.setdefault(ccy_map.get(sym, "USD"), []).append(sym)

    result = {}
    for ccy, syms in ccy_groups.items():
        portfolio_series = None
        for sym in syms:
            if sym not in close_series:
                continue
            col = close_series[sym].reindex(idx).ffill().bfill().dropna()
            weighted = col * qty_map[sym]
            portfolio_series = weighted if portfolio_series is None else portfolio_series.add(weighted, fill_value=0)

        if portfolio_series is not None:
            ps = portfolio_series.dropna()
            ps = ps[ps > 0]
            if ps.empty:
                continue
            result[ccy] = {
                "dates":      [d.date().isoformat() for d in ps.index],
                "values":     [round(float(v), 2) for v in ps.tolist()],
                "total_cost": round(sum(cost_map.get(s, 0) for s in syms if cost_map.get(s)), 2),
            }
    return result


@st.cache_resource
def _fugle_client():
    """Return a Fugle RestClient if API key is configured, else None."""
    import base64
    try:
        import streamlit as _st
        raw = _st.secrets.get("fugle_api_key", "") or ""
    except Exception:
        raw = ""
    if not raw:
        return None
    # The key may be stored base64-encoded; try to decode it.
    try:
        decoded = base64.b64decode(raw).decode("utf-8").strip()
        api_key = decoded.split()[0]   # take the first token if space-separated
    except Exception:
        api_key = raw.strip()
    try:
        from fugle_marketdata import RestClient
        return RestClient(api_key=api_key)
    except Exception:
        return None


def _fetch_tw_prices_fugle(syms: list) -> dict:
    """
    Fetch last prices for a list of .TW symbols using Fugle REST API.
    Returns {sym: price}. Missing entries mean Fugle failed for that symbol.
    """
    client = _fugle_client()
    if client is None:
        return {}

    def _fetch_one(sym):
        fugle_sym = sym.removesuffix(".TW")
        try:
            data = client.stock.intraday.quote(symbol=fugle_sym)
            price = (
                (data.get("lastTrade") or {}).get("price")
                or data.get("closePrice")
                or data.get("previousClose")
                or 0.0
            )
            return (sym, float(price)) if price else None
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=min(len(syms), 6)) as pool:
        results = pool.map(_fetch_one, syms)
    return {sym: p for sym, p in (r for r in results if r) if p > 0}


def _finnhub_api_key() -> str:
    """Return Finnhub API key from st.secrets or empty string."""
    try:
        import streamlit as _st
        return _st.secrets.get("finnhub_api_key", "") or ""
    except Exception:
        return ""


def _fetch_us_prices_finnhub(syms: list) -> dict:
    """
    Fetch last prices for US tickers using Finnhub /quote endpoint.
    Returns {sym: price}. Falls back gracefully on error.
    """
    import urllib.request as _ur
    import urllib.parse  as _up
    key = _finnhub_api_key()
    if not key:
        return {}

    def _fetch_one(sym):
        try:
            url = f"https://finnhub.io/api/v1/quote?symbol={_up.quote(sym)}&token={key}"
            req = _ur.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with _ur.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
            price = data.get("c") or data.get("pc") or 0.0
            return (sym, float(price)) if price else None
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=min(len(syms), 6)) as pool:
        results = pool.map(_fetch_one, syms)
    return {sym: p for sym, p in (r for r in results if r) if p > 0}


@st.cache_data(ttl=86400, show_spinner=False)
def _fetch_sector_dividend_info(syms_json: str) -> dict:
    """
    Fetch sector and dividend info for a list of tickers (cached 24 hours).
    Returns {sym: {"sector": str|None, "dividend_yield": float|None, "dividend_rate": float|None}}
    """
    syms = json.loads(syms_json)

    def _fetch_one(sym):
        try:
            info = yf.Ticker(sym).info
            return sym, {
                "sector": info.get("sector") or None,
                "dividend_yield": info.get("dividendYield") or None,
                "dividend_rate": info.get("dividendRate") or None,
                "52w_high": info.get("fiftyTwoWeekHigh") or None,
                "52w_low": info.get("fiftyTwoWeekLow") or None,
            }
        except Exception:
            return sym, {"sector": None, "dividend_yield": None, "dividend_rate": None, "52w_high": None, "52w_low": None}

    with ThreadPoolExecutor(max_workers=min(len(syms), 8)) as pool:
        results = pool.map(_fetch_one, syms)
    return dict(results)


def _market_active_tickers(all_syms: list) -> list:
    """
    Return the subset of tickers whose market is likely open right now.
    Uses Taiwan time (UTC+8) as reference:
      - TW/HK session: 08:30–14:00 CST
      - US session:    21:30–05:30 CST (covers EST and EDT)
    When only one market is open, the other market's tickers are skipped
    (caller should use cached prices for them instead).
    """
    tw_tz = timezone(timedelta(hours=8))
    now   = datetime.now(tw_tz)
    if now.weekday() >= 5:          # weekend — nothing is trading
        return []
    h = now.hour + now.minute / 60  # decimal hour in CST
    tw_syms = [s for s in all_syms if s.endswith(".TW") or s.endswith(".HK")]
    us_syms = [s for s in all_syms if not s.endswith(".TW") and not s.endswith(".HK")]
    # Only narrow down when we have both types in the portfolio
    if tw_syms and us_syms:
        tw_open = 8.5 <= h <= 14.0
        us_open = h >= 21.5 or h <= 5.5
        if tw_open and not us_open:
            return tw_syms
        if us_open and not tw_open:
            return us_syms
    return all_syms


# =============================================================================
#  MAIN APP
# =============================================================================

# -- Auto-refresh (price only) ------------------------------------------------
st_autorefresh(
    interval=st.session_state.refresh_interval * 1000,
    key="price_autorefresh",
)

# -- Custom CSS ---------------------------------------------------------------
st.markdown("""
<style>
/* ══════════════════════════════════════════
   GLOBAL
══════════════════════════════════════════ */
html, body, [class*="css"] { font-family: "Inter", "Segoe UI", sans-serif; }

/* Reduce Streamlit default top padding */
.block-container { padding-top: 1.6rem !important; padding-bottom: 2rem !important; }

/* Hide Streamlit's default menu/footer */
#MainMenu, footer { visibility: hidden; }

/* ══════════════════════════════════════════
   METRIC CARDS
══════════════════════════════════════════ */
.metric-card {
    background: linear-gradient(145deg, #161c2d, #1a2235);
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 14px;
    padding: 20px 22px 18px;
    box-shadow: 0 2px 16px rgba(0,0,0,0.4);
    margin-bottom: 6px;
    position: relative;
    overflow: hidden;
}
.metric-card::before {
    content: "";
    position: absolute;
    top: 0; left: 0;
    width: 3px; height: 100%;
    background: linear-gradient(180deg, #4e8cff, #7eb8f7);
    border-radius: 3px 0 0 3px;
}
.metric-card.pnl-pos::before { background: linear-gradient(180deg, #34d399, #6ee7b7); }
.metric-card.pnl-neg::before { background: linear-gradient(180deg, #f87171, #fca5a5); }
.metric-card .m-label {
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 1.4px;
    text-transform: uppercase;
    color: #5a6a8a;
    margin-bottom: 8px;
}
.metric-card .m-value {
    font-size: 28px;
    font-weight: 700;
    color: #dce3f0;
    letter-spacing: -0.5px;
    line-height: 1.15;
}
.metric-card .m-delta {
    margin-top: 8px;
    display: inline-flex;
    align-items: center;
    gap: 4px;
    font-size: 12px;
    font-weight: 600;
    padding: 3px 10px;
    border-radius: 20px;
}
.m-delta-pos { background: rgba(52,211,153,0.12); color: #6ee7b7; }
.m-delta-neg { background: rgba(248,113,113,0.12); color: #fca5a5; }

/* ══════════════════════════════════════════
   FX UNIFIED CARD
══════════════════════════════════════════ */
.fx-card {
    background: linear-gradient(135deg, #161c2d, #1a2235);
    border: 1px solid rgba(78,140,255,0.2);
    border-radius: 14px;
    padding: 18px 22px;
    margin: 12px 0 4px;
    display: flex;
    align-items: center;
    gap: 24px;
    flex-wrap: wrap;
}
.fx-badge {
    font-size: 11px; font-weight: 700; letter-spacing: 1px;
    text-transform: uppercase; color: #4e8cff;
    background: rgba(78,140,255,0.1);
    border-radius: 6px; padding: 3px 10px;
}
.fx-val   { font-size: 26px; font-weight: 700; color: #dce3f0; }
.fx-pnl-p { font-size: 26px; font-weight: 700; color: #6ee7b7; }
.fx-pnl-n { font-size: 26px; font-weight: 700; color: #fca5a5; }
.fx-rate  { font-size: 11px; color: #4a5568; align-self: flex-end; margin-left: auto; }

/* ══════════════════════════════════════════
   SECTION TITLES
══════════════════════════════════════════ */
.section-title {
    font-size: 16px;
    font-weight: 700;
    color: #c8d0e0;
    padding: 8px 0 8px 14px;
    border-left: 3px solid #4e8cff;
    margin: 28px 0 14px;
    letter-spacing: 0.2px;
}

/* ══════════════════════════════════════════
   TRANSACTION TABLE
══════════════════════════════════════════ */
.txn-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
}
.txn-table th {
    text-align: left;
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 1.2px;
    text-transform: uppercase;
    color: #4a5568;
    padding: 8px 12px;
    border-bottom: 1px solid rgba(255,255,255,0.06);
}
.txn-table td {
    padding: 9px 12px;
    border-bottom: 1px solid rgba(255,255,255,0.04);
    color: #c8d0e0;
}
.txn-table tr:last-child td { border-bottom: none; }
.txn-table tr:hover td { background: rgba(255,255,255,0.025); }
.badge-buy  { background: rgba(52,211,153,0.12); color: #6ee7b7; border-radius: 20px; padding: 2px 10px; font-weight: 600; font-size: 11px; }
.badge-sell { background: rgba(248,113,113,0.12); color: #fca5a5; border-radius: 20px; padding: 2px 10px; font-weight: 600; font-size: 11px; }

/* ══════════════════════════════════════════
   NEWS CARDS
══════════════════════════════════════════ */
.gn-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 10px;
    margin-top: 4px;
}
.gn-card {
    background: #161c2d;
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 12px;
    overflow: hidden;
    display: flex;
    flex-direction: column;
    transition: border-color 0.2s, transform 0.15s, box-shadow 0.2s;
}
.gn-card:hover {
    border-color: rgba(78,140,255,0.35);
    transform: translateY(-2px);
    box-shadow: 0 6px 20px rgba(0,0,0,0.3);
}
.gn-thumb {
    width: 100%; height: 120px; object-fit: cover; display: block;
}
.gn-thumb-placeholder {
    width: 100%; height: 120px;
    background: linear-gradient(135deg, #1a2132, #1e2840);
    display: flex; align-items: center; justify-content: center; font-size: 28px;
}
.gn-body { padding: 11px 13px 13px; display: flex; flex-direction: column; flex: 1; }
.gn-title a {
    font-size: 13px; font-weight: 600; color: #dce3f0;
    text-decoration: none; line-height: 1.45;
    display: -webkit-box; -webkit-line-clamp: 3;
    -webkit-box-orient: vertical; overflow: hidden;
}
.gn-title a:hover { color: #7eb8f7; }
.gn-meta { margin-top: 8px; display: flex; align-items: center; gap: 6px; }
.gn-source {
    font-size: 10px; font-weight: 600; color: #7eb8f7;
    background: rgba(78,140,255,0.08);
    border-radius: 4px; padding: 2px 7px;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 110px;
}
.gn-time { font-size: 10px; color: #4a5568; white-space: nowrap; }

/* ══════════════════════════════════════════
   SIDEBAR
══════════════════════════════════════════ */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0f131e 0%, #131722 100%);
    border-right: 1px solid rgba(255,255,255,0.05);
}
[data-testid="stSidebar"] .stButton > button {
    width: 100%;
    background: rgba(78,140,255,0.08);
    border: 1px solid rgba(78,140,255,0.2);
    color: #7eb8f7;
    border-radius: 8px;
    font-size: 12px;
    font-weight: 600;
    padding: 6px 12px;
    transition: background 0.2s;
}
[data-testid="stSidebar"] .stButton > button:hover {
    background: rgba(78,140,255,0.18);
}
/* Expander header */
[data-testid="stSidebar"] details summary {
    font-size: 13px !important;
    font-weight: 600 !important;
    color: #8a9ab8 !important;
}

/* ══════════════════════════════════════════
   PAGE TITLE
══════════════════════════════════════════ */
[data-testid="stAppViewContainer"] h1 {
    font-weight: 800;
    letter-spacing: -0.8px;
    background: linear-gradient(135deg, #dce3f0 30%, #7eb8f7 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}

/* ══════════════════════════════════════════
   STREAMLIT TABS
══════════════════════════════════════════ */
button[data-baseweb="tab"] {
    font-size: 12px !important;
    font-weight: 600 !important;
    color: #6b7a99 !important;
    padding: 6px 14px !important;
}
button[data-baseweb="tab"][aria-selected="true"] {
    color: #7eb8f7 !important;
    border-bottom-color: #4e8cff !important;
}

/* ══════════════════════════════════════════
   DIVIDER
══════════════════════════════════════════ */
hr { border-color: rgba(255,255,255,0.05) !important; margin: 20px 0 !important; }
</style>
""", unsafe_allow_html=True)

# -- Sidebar ------------------------------------------------------------------
with st.sidebar:
    st.title(t("sidebar_title"))

    # Language toggle
    if st.button(t("lang_toggle"), key="lang_btn"):
        st.session_state.lang = "en" if st.session_state.lang == "zh" else "zh"
        st.rerun()

    with st.form("add_form", clear_on_submit=True):
        st.subheader(t("add_form_title"))
        ticker_input = st.text_input(
            t("ticker_label"),
            placeholder=t("ticker_placeholder"),
        )
        st.caption(t("ticker_caption"))
        qty_input = st.number_input(t("shares_label"), min_value=0.01, step=1.0, value=1.0)
        price_input = st.number_input(
            t("avg_price_label"),
            min_value=0.0,
            step=0.01,
            value=0.0,
        )
        add_btn = st.form_submit_button(t("add_btn"))

    if add_btn and ticker_input.strip():
        sym = normalize_ticker(ticker_input.strip())
        st.session_state.portfolio[sym] = {"quantity": qty_input, "avg_price": price_input}
        upsert_holding(st.session_state.user_id, sym, qty_input, price_input)
        st.success(t("added_msg").format(sym))

    if st.session_state.portfolio:
        st.divider()
        st.subheader(t("edit_title"))
        edit_sym = st.selectbox(t("edit_select"), list(st.session_state.portfolio), key="edit_sym")
        current = st.session_state.portfolio[edit_sym]
        with st.form("edit_form", clear_on_submit=False):
            new_qty = st.number_input(
                t("shares_label"),
                min_value=0.01, step=1.0,
                value=float(current["quantity"]),
            )
            new_price = st.number_input(
                t("avg_price_edit_label"),
                min_value=0.0, step=0.01,
                value=float(current["avg_price"]),
            )
            save_btn = st.form_submit_button(t("save_btn"))
        if save_btn:
            st.session_state.portfolio[edit_sym] = {"quantity": new_qty, "avg_price": new_price}
            upsert_holding(st.session_state.user_id, edit_sym, new_qty, new_price)
            st.success(t("updated_msg").format(edit_sym))
            st.rerun()

        st.divider()
        st.subheader(t("remove_title"))
        remove_sym = st.selectbox(t("remove_select"), list(st.session_state.portfolio), key="remove_sym")
        if st.button(t("remove_btn")):
            del st.session_state.portfolio[remove_sym]
            delete_holding(st.session_state.user_id, remove_sym)
            st.rerun()

    # -- Transaction form --
    st.divider()
    st.subheader(t("trans_section"))
    with st.expander(t("trans_add_title"), expanded=False):
        with st.form("add_txn_form", clear_on_submit=True):
            txn_sym_input = st.text_input(t("trans_ticker_label"), placeholder="AAPL / 0050")
            txn_type      = st.radio(t("trans_type_label"),
                                     [t("trans_buy"), t("trans_sell")],
                                     horizontal=True, key="txn_type_radio")
            txn_date  = st.date_input(t("trans_date_label"), value=datetime.now().date())
            txn_qty   = st.number_input(t("trans_qty_label"),  min_value=0.01, step=1.0, value=1.0)
            txn_price = st.number_input(t("trans_price_label"), min_value=0.01, step=0.01, value=1.0)
            txn_submit = st.form_submit_button(t("trans_submit_btn"))
        if txn_submit and txn_sym_input.strip():
            _sym       = normalize_ticker(txn_sym_input.strip())
            _ttype     = "buy" if txn_type == t("trans_buy") else "sell"
            _cur_qty   = (st.session_state.portfolio.get(_sym) or {}).get("quantity", 0)
            if _ttype == "sell" and txn_qty > _cur_qty:
                st.error(t("trans_error_sell"))
            else:
                add_transaction(st.session_state.user_id, _sym, txn_date, txn_qty, txn_price, _ttype)
                st.session_state.portfolio = load_holdings(st.session_state.user_id)
                st.session_state.cached_news = {}
                st.session_state.news_portfolio_key = ""
                st.session_state.pop("cached_txns", None)
                st.success(t("trans_added").format(_sym, t("trans_buy_label") if _ttype == "buy" else t("trans_sell_label"), txn_qty, txn_price))
                st.rerun()

    # -- Price Alerts form --
    if st.session_state.portfolio:
        st.divider()
        st.subheader(t("alert_title"))
        _alert_sym = st.selectbox(t("alert_select"), list(st.session_state.portfolio), key="alert_sym")
        _existing_alerts = st.session_state.get("cached_alerts") or load_price_alerts(st.session_state.user_id)
        _cur_alert = _existing_alerts.get(_alert_sym, {})
        with st.form("alert_form", clear_on_submit=False):
            _alert_target = st.number_input(
                t("alert_target_label"), min_value=0.0, step=0.01,
                value=float(_cur_alert.get("target_price") or 0),
            )
            _alert_stop = st.number_input(
                t("alert_stop_label"), min_value=0.0, step=0.01,
                value=float(_cur_alert.get("stop_price") or 0),
            )
            _alert_save = st.form_submit_button(t("alert_save_btn"))
        if _alert_save:
            upsert_price_alert(
                st.session_state.user_id, _alert_sym,
                _alert_target if _alert_target > 0 else None,
                _alert_stop if _alert_stop > 0 else None,
            )
            st.success(t("alert_saved").format(_alert_sym))
            st.session_state.pop("cached_alerts", None)
            st.rerun()
        if _cur_alert.get("target_price") or _cur_alert.get("stop_price"):
            if st.button(t("alert_clear_btn"), key="alert_clear_btn"):
                delete_price_alert(st.session_state.user_id, _alert_sym)
                st.success(t("alert_cleared").format(_alert_sym))
                st.session_state.pop("cached_alerts", None)
                st.rerun()
        # Show current alerts summary
        if _existing_alerts:
            st.caption(f"📋 {t('alert_current')}:")
            for _a_sym, _a_val in _existing_alerts.items():
                _parts = []
                if _a_val.get("target_price"):
                    _parts.append(f"🎯{t('alert_target_short')} {_a_val['target_price']:.2f}")
                if _a_val.get("stop_price"):
                    _parts.append(f"🛑{t('alert_stop_short')} {_a_val['stop_price']:.2f}")
                if _parts:
                    st.caption(f"  **{_a_sym}** — {' / '.join(_parts)}")

    st.divider()
    st.subheader(t("refresh_title"))
    st.session_state.refresh_interval = st.slider(
        t("refresh_label"),
        min_value=5,
        max_value=60,
        value=max(5, st.session_state.refresh_interval),
        help=t("refresh_help"),
    )
    st.divider()
    st.caption(f"🔑 Your ID: `{browser_id[:8]}…`")
    st.caption(t("id_caption"))

# -- Main title ---------------------------------------------------------------
st.title(t("app_title"))

if not st.session_state.portfolio:
    st.info(t("empty_info"))
    st.markdown(t("supported_formats"))
    st.stop()

# -- Fetch prices (every refresh) ---------------------------------------------
rows = []
fetch_errors = []

# Invalidate news cache when portfolio changes
current_key = ",".join(sorted(st.session_state.portfolio.keys()))
if current_key != st.session_state.news_portfolio_key:
    st.session_state.cached_news = {}
    st.session_state.news_portfolio_key = current_key

_all_tickers   = list(st.session_state.portfolio.keys())
_fetch_tickers = _market_active_tickers(_all_tickers)
_skip_tickers  = set(_all_tickers) - set(_fetch_tickers)

# Show which market session is active
if _skip_tickers:
    _tw_active = any(s.endswith(".TW") or s.endswith(".HK") for s in _fetch_tickers)
    st.caption(t("market_tw_only") if _tw_active else t("market_us_only"))
else:
    st.caption(t("market_both"))

with st.spinner(t("fetching_prices")):
    batch_prices: dict[str, float] = {}
    if _fetch_tickers:
        # --- Fugle for TW stocks (real-time) ---------------------------------
        _tw_fetch = [s for s in _fetch_tickers if s.endswith(".TW")]
        if _tw_fetch:
            batch_prices.update(_fetch_tw_prices_fugle(_tw_fetch))

        # --- Finnhub for US stocks (real-time) --------------------------------
        _us_fetch = [s for s in _fetch_tickers
                     if not s.endswith(".TW") and not s.endswith(".HK")]
        if _us_fetch:
            batch_prices.update(_fetch_us_prices_finnhub(_us_fetch))

        # --- yfinance fallback for HK + anything Fugle/Finnhub missed --------
        _yf_needed = [s for s in _fetch_tickers if s not in batch_prices]
        if _yf_needed:
            try:
                _raw = yf.download(
                    _yf_needed,
                    period="1d",
                    interval="1m",
                    progress=False,
                    auto_adjust=True,
                    group_by="ticker",
                )
                if len(_yf_needed) == 1:
                    _s = _yf_needed[0]
                    try:
                        batch_prices[_s] = float(_raw["Close"].dropna().iloc[-1])
                    except Exception:
                        pass
                else:
                    for _s in _yf_needed:
                        try:
                            batch_prices[_s] = float(_raw[_s]["Close"].dropna().iloc[-1])
                        except Exception:
                            pass
            except Exception:
                pass

    for sym, holding in st.session_state.portfolio.items():
        # Use fresh price if fetched; fall back to session-state cache for skipped tickers
        if sym in batch_prices:
            price = batch_prices[sym]
        elif sym in _skip_tickers and sym in st.session_state.last_prices:
            price = st.session_state.last_prices[sym]
        else:
            price = 0.0

        # Hard-code currencies by suffix to avoid extra API calls
        currency = st.session_state.last_currencies.get(sym, "N/A")
        if currency == "N/A":
            if sym.endswith(".TW"):
                currency = "TWD"
            elif sym.endswith(".HK"):
                currency = "HKD"
            else:
                currency = "USD"

        if price == 0.0:
            try:
                fi = yf.Ticker(sym).fast_info
                price = fi.last_price or 0.0
            except Exception as e:
                if sym not in _skip_tickers:
                    fetch_errors.append(f"{sym}: {e}")

        # Persist to session-state cache
        if price > 0:
            st.session_state.last_prices[sym] = price
        if currency != "N/A":
            st.session_state.last_currencies[sym] = currency

        qty     = holding["quantity"]
        avg_p   = holding["avg_price"]
        mkt_val = price * qty
        cost    = avg_p * qty if avg_p > 0 else None
        pnl     = (mkt_val - cost) if cost else None
        pnl_pct = (pnl / cost * 100) if cost else None

        rows.append({
            "sym": sym, "qty": qty, "avg_p": avg_p,
            "price": price, "mkt_val": mkt_val, "currency": currency,
            "cost": cost, "pnl": pnl, "pnl_pct": pnl_pct,
        })

# -- Fetch news (cached, only refreshed on portfolio change) ------------------
def _normalize_yf_article(raw: dict) -> Optional[dict]:
    """Normalize both old and new yfinance news formats to a flat dict."""
    # New format (yfinance >= ~0.2.50): {"id":..., "content":{...}}
    if "content" in raw and isinstance(raw["content"], dict):
        c = raw["content"]
        title = c.get("title", "")
        link  = (c.get("canonicalUrl") or c.get("clickThroughUrl") or {}).get("url", "")
        pub   = (c.get("provider") or {}).get("displayName", "")
        ts    = None
        try:
            from datetime import datetime as _dt
            ts = int(_dt.fromisoformat(c["pubDate"].replace("Z", "+00:00")).timestamp())
        except Exception:
            pass
        thumb = c.get("thumbnail")
        if thumb and "resolutions" not in thumb and "originalUrl" in thumb:
            thumb = {"resolutions": [{"url": thumb["originalUrl"], "width": 300}]}
        out = {"title": title, "link": link, "publisher": pub, "providerPublishTime": ts}
        if thumb:
            out["thumbnail"] = thumb
        return out if title and link else None
    # Old flat format
    title = raw.get("title", "")
    link  = raw.get("link", "")
    return raw if title and link else None

def _fetch_news(sym: str) -> list:
    """Fetch up to 20 news items: yf.Ticker.news first, then Yahoo RSS top-up."""
    articles = []
    seen_links = set()

    # Source 1: yfinance built-in (handles both old and new format)
    try:
        for raw in (yf.Ticker(sym).news or []):
            item = _normalize_yf_article(raw)
            if item and item["link"] not in seen_links:
                seen_links.add(item["link"])
                articles.append(item)
    except Exception:
        pass

    # Source 2: Yahoo Finance RSS feed (gives up to ~20 more items)
    try:
        import urllib.request, xml.etree.ElementTree as ET
        rss_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={sym}&region=US&lang=en-US"
        req = urllib.request.Request(rss_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=6) as resp:
            tree = ET.fromstring(resp.read())
        ns = {"media": "http://search.yahoo.com/mrss/"}
        for item in tree.findall(".//item"):
            # <link> in RSS 2.0 is sometimes tail text, not element text
            link = ""
            for child in item:
                if child.tag == "link":
                    link = (child.text or child.tail or "").strip()
                    break
            if not link:
                link = (item.findtext("link") or "").strip()
            title = (item.findtext("title") or "").strip()
            src   = item.findtext("source") or ""
            pub   = item.findtext("pubDate") or ""
            if not link or not title or link in seen_links:
                continue
            seen_links.add(link)
            ts = None
            try:
                from email.utils import parsedate_to_datetime
                ts = int(parsedate_to_datetime(pub).timestamp())
            except Exception:
                pass
            mc = item.find("media:content", ns)
            art: dict = {"title": title, "link": link, "publisher": src, "providerPublishTime": ts}
            if mc is not None and mc.get("url"):
                art["thumbnail"] = {"resolutions": [{"url": mc.get("url"), "width": 300}]}
            articles.append(art)
    except Exception:
        pass

    return articles[:20]

if st.session_state.portfolio:
    # Only fetch news for the currently-active market session
    _active_news_syms = set(_fetch_tickers)
    missing_syms = [
        sym for sym in st.session_state.portfolio
        if sym not in st.session_state.cached_news and sym in _active_news_syms
    ]
    if missing_syms:
        with st.spinner(t("fetching_news")):
            with ThreadPoolExecutor(max_workers=min(len(missing_syms), 6)) as pool:
                news_results = list(pool.map(lambda s: (s, _fetch_news(s)), missing_syms))
            for sym, items in news_results:
                if items:
                    st.session_state.cached_news[sym] = items

for err in fetch_errors:
    st.warning(t("fetch_failed").format(err))

# -- Price Alert Banners -------------------------------------------------------
if "cached_alerts" not in st.session_state:
    st.session_state.cached_alerts = load_price_alerts(st.session_state.user_id)
_price_alerts = st.session_state.cached_alerts
if _price_alerts and rows:
    _row_prices = {r["sym"]: r["price"] for r in rows if r["price"] > 0}
    for _pa_sym, _pa_vals in _price_alerts.items():
        _pa_price = _row_prices.get(_pa_sym)
        if not _pa_price:
            continue
        if _pa_vals.get("target_price") and _pa_price >= _pa_vals["target_price"]:
            st.success(t("alert_target_hit").format(
                sym=_pa_sym, price=_pa_price, target=_pa_vals["target_price"]))
        if _pa_vals.get("stop_price") and _pa_price <= _pa_vals["stop_price"]:
            st.error(t("alert_stop_hit").format(
                sym=_pa_sym, price=_pa_price, stop=_pa_vals["stop_price"]))

# -- Portfolio summary --------------------------------------------------------
st.markdown(f'<p class="section-title">{t("sec_overview")}</p>', unsafe_allow_html=True)

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

# Build flat list of metric cards (Value + P&L per currency)
metric_cards = []
for ccy in currencies:
    total_val  = by_currency_val[ccy]
    total_pnl  = by_currency_pnl.get(ccy)
    total_cost = by_currency_cost.get(ccy, 0)
    pnl_pct    = (total_pnl / total_cost * 100) if (total_pnl is not None and total_cost > 0) else None
    metric_cards.append({"label": t("total_value").format(ccy), "value": f"{total_val:,.2f}", "delta": None, "positive": True, "is_pnl": False})
    if total_pnl is not None:
        arrow    = "▲" if total_pnl >= 0 else "▼"
        delta_txt = f"{arrow} {pnl_pct:+.2f}%" if pnl_pct is not None else None
        metric_cards.append({
            "label": t("total_pnl").format(ccy),
            "value": f"{total_pnl:+,.2f}",
            "delta": delta_txt,
            "positive": total_pnl >= 0,
            "is_pnl": True,
        })

card_cols = st.columns(max(len(metric_cards), 1))
for col, card in zip(card_cols, metric_cards):
    value_color = ""
    extra_cls   = ""
    if card.get("is_pnl"):
        value_color = "color:#6ee7b7;" if card["positive"] else "color:#fca5a5;"
        extra_cls   = "pnl-pos" if card["positive"] else "pnl-neg"
    delta_html = ""
    if card["delta"]:
        cls = "m-delta-pos" if card["positive"] else "m-delta-neg"
        delta_html = f'<div class="m-delta {cls}">{card["delta"]}</div>'
    col.markdown(f"""
<div class="metric-card {extra_cls}">
    <div class="m-label">{card["label"]}</div>
    <div class="m-value" style="{value_color}">{card["value"]}</div>
    {delta_html}
</div>
""", unsafe_allow_html=True)

# -- FX Unified Total ---------------------------------------------------------
_has_twd = bool(by_currency_val.get("TWD"))
_has_usd = bool(by_currency_val.get("USD"))
if _has_twd and _has_usd:
    _usdtwd = _fetch_fx_rate("USDTWD=X")
    if _usdtwd:
        _fx_ccy_col, _fx_spacer = st.columns([2, 6])
        with _fx_ccy_col:
            _fx_choice = st.radio(t("fx_label"), ["TWD", "USD"], horizontal=True,
                                  key="fx_choice", label_visibility="visible")
        _twd_val  = by_currency_val.get("TWD", 0);  _usd_val  = by_currency_val.get("USD", 0)
        _twd_cost = by_currency_cost.get("TWD", 0); _usd_cost = by_currency_cost.get("USD", 0)
        _twd_pnl  = by_currency_pnl.get("TWD", 0);  _usd_pnl  = by_currency_pnl.get("USD", 0)
        if _fx_choice == "TWD":
            _grand_val  = _twd_val  + _usd_val  * _usdtwd
            _grand_cost = _twd_cost + _usd_cost * _usdtwd
            _grand_pnl  = _twd_pnl  + _usd_pnl  * _usdtwd
        elif _usdtwd > 0:
            _grand_val  = _twd_val  / _usdtwd + _usd_val
            _grand_cost = _twd_cost / _usdtwd + _usd_cost
            _grand_pnl  = _twd_pnl  / _usdtwd + _usd_pnl
        else:
            _grand_val = _grand_cost = _grand_pnl = 0
        _grand_pnl_pct = (_grand_pnl / _grand_cost * 100) if _grand_cost > 0 else None
        _arrow    = "▲" if _grand_pnl >= 0 else "▼"
        _pnl_cls  = "fx-pnl-p" if _grand_pnl >= 0 else "fx-pnl-n"
        _dcls     = "m-delta-pos" if _grand_pnl >= 0 else "m-delta-neg"
        _pct_str  = f"&nbsp;<span class='m-delta {_dcls}'>{_arrow} {_grand_pnl_pct:+.2f}%</span>" if _grand_pnl_pct is not None else ""
        st.markdown(f"""
<div class="fx-card">
  <span class="fx-badge">{_fx_choice}</span>
  <div>
    <div style="font-size:10px;font-weight:700;letter-spacing:1.2px;text-transform:uppercase;color:#4a5568;margin-bottom:4px">{t("fx_unified_total").format(_fx_choice)}</div>
    <span class="fx-val">{_grand_val:,.0f}</span>{_pct_str}
  </div>
  <div style="width:1px;background:rgba(255,255,255,0.06);align-self:stretch"></div>
  <div>
    <div style="font-size:10px;font-weight:700;letter-spacing:1.2px;text-transform:uppercase;color:#4a5568;margin-bottom:4px">{t("fx_unified_pnl").format(_fx_choice)}</div>
    <span class="{_pnl_cls}">{_grand_pnl:+,.0f}</span>
  </div>
  <span class="fx-rate">{t("fx_rate_note").format(_usdtwd)}</span>
</div>""", unsafe_allow_html=True)
    else:
        st.caption(t("fx_unavailable"))

# -- Fetch sector/dividend/52w info (shared by table, sector pie, dividend) ----
_syms_for_sd = json.dumps(sorted(list(st.session_state.portfolio.keys())))
with st.spinner(t("sector_fetching")):
    _sd_info = _fetch_sector_dividend_info(_syms_for_sd)

# -- Holdings table -----------------------------------------------------------
st.markdown(f'<p class="section-title">{t("sec_detail")}</p>', unsafe_allow_html=True)

def _color_pnl(series):
    out = []
    for v in series:
        if pd.isna(v):
            out.append("")
        elif v > 0:
            out.append("color: #6ee7b7; font-weight: 600")
        elif v < 0:
            out.append("color: #fca5a5; font-weight: 600")
        else:
            out.append("")
    return out

def _make_styled_df(subset_rows):
    col_ticker    = t("col_ticker")
    col_shares    = t("col_shares")
    col_avg_price = t("col_avg_price")
    col_cur_price = t("col_current_price")
    col_mkt_val   = t("col_mkt_val")
    col_pnl       = t("col_pnl")
    col_pnl_pct   = t("col_pnl_pct")
    col_52w_high  = t("col_52w_high")
    col_52w_low   = t("col_52w_low")
    col_52w_range = t("col_52w_range")
    data = []
    for r in subset_rows:
        _sdi = _sd_info.get(r["sym"]) or {}
        _hi  = _sdi.get("52w_high")
        _lo  = _sdi.get("52w_low")
        # Position within 52w range as percentage
        if _hi and _lo and _hi != _lo and r["price"] > 0:
            _range_pct = (r["price"] - _lo) / (_hi - _lo) * 100
        else:
            _range_pct = float("nan")
        data.append({
            col_ticker:    r["sym"],
            col_shares:    r["qty"],
            col_avg_price: r["avg_p"] if r["avg_p"] > 0 else float("nan"),
            col_cur_price: r["price"],
            col_52w_high:  _hi if _hi else float("nan"),
            col_52w_low:   _lo if _lo else float("nan"),
            col_52w_range: _range_pct,
            col_mkt_val:   r["mkt_val"],
            col_pnl:       r["pnl"] if r["pnl"] is not None else float("nan"),
            col_pnl_pct:   r["pnl_pct"] if r["pnl_pct"] is not None else float("nan"),
        })
    df = pd.DataFrame(data)
    return (
        df.style
        .apply(_color_pnl, subset=[col_pnl, col_pnl_pct])
        .format({
            col_avg_price: lambda x: f"{x:.2f}" if not pd.isna(x) else "—",
            col_cur_price: "{:.2f}",
            col_52w_high:  lambda x: f"{x:.2f}" if not pd.isna(x) else "—",
            col_52w_low:   lambda x: f"{x:.2f}" if not pd.isna(x) else "—",
            col_52w_range: lambda x: f"{x:.0f}%" if not pd.isna(x) else "—",
            col_mkt_val:   "{:,.2f}",
            col_pnl:       lambda x: f"{x:+,.2f}" if not pd.isna(x) else "—",
            col_pnl_pct:   lambda x: f"{x:+.2f}%" if not pd.isna(x) else "—",
            col_shares:    "{:g}",
        })
        .set_properties(
            subset=[col_shares, col_avg_price, col_cur_price, col_mkt_val,
                    col_pnl, col_pnl_pct, col_52w_high, col_52w_low, col_52w_range],
            **{"text-align": "right"},
        )
        .set_properties(subset=[col_ticker], **{"font-weight": "700"})
    )

tw_rows = [r for r in rows if r["currency"] == "TWD"]
us_rows = [r for r in rows if r["currency"] == "USD"]
hk_rows = [r for r in rows if r["currency"] not in ("TWD", "USD")]

_detail_cols = []
_detail_data = []
if tw_rows:
    _detail_cols.append(("tw_stocks", tw_rows, "no_tw"))
if us_rows:
    _detail_cols.append(("us_stocks", us_rows, "no_us"))
if hk_rows:
    _detail_cols.append(("hk_stocks", hk_rows, "no_hk"))
if not _detail_cols:
    _detail_cols = [("tw_stocks", [], "no_tw"), ("us_stocks", [], "no_us")]

_d_cols = st.columns(len(_detail_cols))
for _dc, (_lbl_key, _sub_rows, _empty_key) in zip(_d_cols, _detail_cols):
    with _dc:
        st.markdown(f"**{t(_lbl_key)}**")
        if _sub_rows:
            st.dataframe(_make_styled_df(_sub_rows), width='stretch', hide_index=True)
        else:
            st.caption(t(_empty_key))

# -- Stock price chart (hybrid: plotly for TW/HK, TradingView for US) --------
st.markdown(f'<p class="section-title">{t("sec_chart")}</p>', unsafe_allow_html=True)

from plotly.subplots import make_subplots

_PERIOD_VALS = [
    ("7d",  "1d"),
    ("1mo", "1d"),
    ("3mo", "1d"),
    ("6mo", "1d"),
    ("1y",  "1d"),
    ("2y",  "1wk"),
]

@st.cache_data(ttl=86400, show_spinner=False)
def _to_tv_symbol_us(sym: str) -> str:
    """Return TradingView exchange:symbol for US tickers (cached 24h)."""
    try:
        exchange = getattr(yf.Ticker(sym).fast_info, "exchange", "") or ""
        tv_map = {
            "NMS": "NASDAQ", "NGM": "NASDAQ", "NCM": "NASDAQ",
            "NYQ": "NYSE",   "NYE": "NYSE",
            "PCX": "AMEX",   "ASE": "AMEX",
        }
        return f"{tv_map.get(exchange.upper(), 'NASDAQ')}:{sym}"
    except Exception:
        return f"NASDAQ:{sym}"

@st.cache_data(ttl=300)
def _fetch_ohlcv(sym: str, period_str: str, interval_str: str) -> pd.DataFrame:
    df = yf.download(sym, period=period_str, interval=interval_str,
                     progress=False, auto_adjust=True)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df.index = pd.to_datetime(df.index)
    return df.dropna()

sym_currency = {r["sym"]: r["currency"] for r in rows}
all_syms     = [r["sym"] for r in rows]

chart_sym      = st.selectbox(t("chart_sym_label"), all_syms, key="chart_sym")
chart_currency = sym_currency.get(chart_sym, "")
is_tw_or_hk    = chart_sym.endswith(".TW") or chart_sym.endswith(".HK")

if is_tw_or_hk:
    _period_opts = t("chart_period_opts")
    chart_period = st.selectbox(t("chart_period_label"), _period_opts, index=2, key="chart_period")
else:
    chart_period = None  # TradingView handles period internally

if not is_tw_or_hk:
    # ── TradingView for US stocks ────────────────────────────────────────────
    tv_sym = _to_tv_symbol_us(chart_sym)
    _tv_sym_safe = json.dumps(tv_sym)
    components.html(
        f"""
        <div class="tradingview-widget-container" style="height:520px; width:100%;">
          <div class="tradingview-widget-container__widget" style="height:520px; width:100%;"></div>
          <script
            type="text/javascript"
            src="https://s3.tradingview.com/external-embedding/embed-widget-advanced-chart.js"
            async>
          {{
            "symbol":            {_tv_sym_safe},
            "interval":          "D",
            "timezone":          "America/New_York",
            "theme":             "dark",
            "style":             "1",
            "locale":            "zh_TW",
            "width":             "100%",
            "height":            520,
            "withdateranges":    true,
            "hide_side_toolbar": false,
            "allow_symbol_change": false,
            "save_image":        true,
            "calendar":          false,
            "studies":           ["STD;MA", "STD;MACD"]
          }}
          </script>
        </div>
        """,
        height=540,
    )
else:
    # ── Plotly candlestick for TW / HK stocks ────────────────────────────────
    _period_opts = t("chart_period_opts")
    period_str, interval_str = _PERIOD_VALS[_period_opts.index(chart_period) if chart_period in _period_opts else 2]
    ohlcv = _fetch_ohlcv(chart_sym, period_str, interval_str)

    if ohlcv.empty:
        st.warning(t("chart_no_data").format(chart_sym))
    else:
        closes = ohlcv["Close"].squeeze()
        opens  = ohlcv["Open"].squeeze()
        highs  = ohlcv["High"].squeeze()
        lows   = ohlcv["Low"].squeeze()
        vols   = ohlcv["Volume"].squeeze() if "Volume" in ohlcv.columns else None
        is_up  = closes >= opens
        vol_colors = ["rgba(110,231,183,0.55)" if u else "rgba(252,165,165,0.55)" for u in is_up]

        # ── Technical indicators ──────────────────────────────────────────────
        # Bollinger Bands (20, 2σ)
        bb_mid  = closes.rolling(20).mean()
        bb_std  = closes.rolling(20).std()
        bb_up   = bb_mid + 2 * bb_std
        bb_dn   = bb_mid - 2 * bb_std

        # RSI (14)
        delta   = closes.diff()
        gain    = delta.clip(lower=0).rolling(14).mean()
        loss    = (-delta.clip(upper=0)).rolling(14).mean()
        rs      = gain / loss.replace(0, float("nan"))
        rsi     = 100 - 100 / (1 + rs)

        # MACD (12, 26, 9)
        ema12      = closes.ewm(span=12, adjust=False).mean()
        ema26      = closes.ewm(span=26, adjust=False).mean()
        macd_line  = ema12 - ema26
        macd_sig   = macd_line.ewm(span=9, adjust=False).mean()
        macd_hist  = macd_line - macd_sig
        hist_colors = ["rgba(110,231,183,0.7)" if v >= 0 else "rgba(252,165,165,0.7)"
                       for v in macd_hist.fillna(0)]

        # Cost basis line for this ticker
        cost_price = next((r["avg_p"] for r in rows if r["sym"] == chart_sym), 0)

        # ── Indicator selector ────────────────────────────────────────────────
        ind_options = ["MA20/50", "Bollinger Bands", "RSI", "MACD"]
        selected_inds = st.multiselect(
            t("chart_indicators_label"), ind_options,
            default=["MA20/50", "Bollinger Bands"],
            key="chart_indicators",
        )

        show_rsi  = "RSI"  in selected_inds
        show_macd = "MACD" in selected_inds

        # ── Build subplot layout ───────────────────────────────────────────────
        n_sub      = 1 + show_rsi + show_macd
        row_h_main = 0.55 if n_sub == 3 else (0.65 if n_sub == 2 else 0.75)
        row_h_vol  = 0.15
        sub_heights = [row_h_main, row_h_vol]
        sub_titles  = ["", "Volume"]
        if show_rsi:
            sub_heights.append((1 - row_h_main - row_h_vol) / (1 + show_macd))
            sub_titles.append("RSI")
        if show_macd:
            sub_heights.append((1 - row_h_main - row_h_vol) / (1 + show_rsi))
            sub_titles.append("MACD")

        total_rows = 2 + show_rsi + show_macd
        rsi_row    = 3 if show_rsi else None
        macd_row   = (3 + show_rsi) if show_macd else None

        fig = make_subplots(
            rows=total_rows, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.03,
            row_heights=sub_heights,
            subplot_titles=sub_titles,
        )

        # ── Row 1: Candlestick ────────────────────────────────────────────────
        fig.add_trace(go.Candlestick(
            x=ohlcv.index, open=opens, high=highs, low=lows, close=closes,
            name="Price",
            increasing=dict(line=dict(color="#6ee7b7", width=1), fillcolor="rgba(110,231,183,0.7)"),
            decreasing=dict(line=dict(color="#fca5a5", width=1), fillcolor="rgba(252,165,165,0.7)"),
            whiskerwidth=0.3,
        ), row=1, col=1)

        # Cost basis horizontal line
        if cost_price > 0:
            fig.add_hline(
                y=cost_price, row=1, col=1,
                line=dict(color="rgba(253,211,77,0.7)", width=1.5, dash="dash"),
                annotation_text=t("chart_cost_label").format(cost_price),
                annotation_font=dict(color="#fcd34d", size=11),
                annotation_position="top left",
            )

        if "MA20/50" in selected_inds:
            for window, color, dash in [(20, "#7eb8f7", "solid"), (50, "#c4b5fd", "dot")]:
                if len(closes) >= window:
                    fig.add_trace(go.Scatter(
                        x=ohlcv.index, y=closes.rolling(window).mean(),
                        name=f"MA{window}",
                        line=dict(color=color, width=1.5, dash=dash),
                        hovertemplate=f"MA{window}: %{{y:.2f}}<extra></extra>",
                    ), row=1, col=1)

        if "Bollinger Bands" in selected_inds and len(closes) >= 20:
            fig.add_trace(go.Scatter(
                x=ohlcv.index, y=bb_up, name="BB Upper",
                line=dict(color="rgba(165,180,252,0.6)", width=1, dash="dot"),
                hovertemplate=f"{t('bb_upper')}: %{{y:.2f}}<extra></extra>",
            ), row=1, col=1)
            fig.add_trace(go.Scatter(
                x=ohlcv.index, y=bb_dn, name="BB Lower",
                line=dict(color="rgba(165,180,252,0.6)", width=1, dash="dot"),
                fill="tonexty",
                fillcolor="rgba(165,180,252,0.06)",
                hovertemplate=f"{t('bb_lower')}: %{{y:.2f}}<extra></extra>",
            ), row=1, col=1)
            fig.add_trace(go.Scatter(
                x=ohlcv.index, y=bb_mid, name="BB Mid",
                line=dict(color="rgba(165,180,252,0.35)", width=1),
                hovertemplate=f"{t('bb_mid')}: %{{y:.2f}}<extra></extra>",
            ), row=1, col=1)

        # ── Row 2: Volume ─────────────────────────────────────────────────────
        if vols is not None:
            fig.add_trace(go.Bar(
                x=ohlcv.index, y=vols, name="Volume",
                marker_color=vol_colors, showlegend=False,
            ), row=2, col=1)

        # ── Row 3 (optional): RSI ─────────────────────────────────────────────
        if show_rsi and rsi_row:
            fig.add_trace(go.Scatter(
                x=ohlcv.index, y=rsi, name="RSI",
                line=dict(color="#fcd34d", width=1.5),
                hovertemplate="RSI: %{y:.1f}<extra></extra>",
            ), row=rsi_row, col=1)
            for level, color in [(70, "rgba(252,165,165,0.4)"), (30, "rgba(110,231,183,0.4)")]:
                fig.add_hline(y=level, row=rsi_row, col=1,
                              line=dict(color=color, width=1, dash="dot"))
            fig.update_yaxes(range=[0, 100], row=rsi_row, col=1)

        # ── Row 4 (optional): MACD ────────────────────────────────────────────
        if show_macd and macd_row:
            fig.add_trace(go.Bar(
                x=ohlcv.index, y=macd_hist, name="MACD Hist",
                marker_color=hist_colors, showlegend=False,
            ), row=macd_row, col=1)
            fig.add_trace(go.Scatter(
                x=ohlcv.index, y=macd_line, name="MACD",
                line=dict(color="#7eb8f7", width=1.5),
                hovertemplate="MACD: %{y:.3f}<extra></extra>",
            ), row=macd_row, col=1)
            fig.add_trace(go.Scatter(
                x=ohlcv.index, y=macd_sig, name="Signal",
                line=dict(color="#f9a8d4", width=1.5),
                hovertemplate="Signal: %{y:.3f}<extra></extra>",
            ), row=macd_row, col=1)

        chart_h = 480 + show_rsi * 130 + show_macd * 130
        fig.update_layout(
            height=chart_h,
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="#0e1117",
            font=dict(color="#c8d0e0", size=12),
            margin=dict(t=24, b=24, l=8, r=8),
            legend=dict(orientation="h", x=0, y=1.04,
                        bgcolor="rgba(0,0,0,0)", font=dict(size=11)),
            xaxis_rangeslider_visible=False,
            hovermode="x unified",
        )
        fig.update_xaxes(gridcolor="rgba(255,255,255,0.04)",
                         showspikes=True, spikecolor="rgba(255,255,255,0.2)",
                         spikethickness=1, spikedash="dot")
        fig.update_yaxes(gridcolor="rgba(255,255,255,0.04)",
                         showspikes=True, spikecolor="rgba(255,255,255,0.2)")
        st.plotly_chart(fig, width='stretch', key="stock_chart")

# -- Pie charts ---------------------------------------------------------------
st.markdown(f'<p class="section-title">{t("sec_pie")}</p>', unsafe_allow_html=True)

_PIE_PALETTE = [
    "#7eb8f7", "#6ee7b7", "#fcd34d", "#fca5a5",
    "#c4b5fd", "#f9a8d4", "#6edfd8", "#fde68a",
    "#a5b4fc", "#fdba74", "#86efac", "#e5e7eb",
]

def _make_pie(subset_rows, flag_title):
    raw_labels = [r["sym"] for r in subset_rows]
    raw_values = [r["mkt_val"] for r in subset_rows]
    currency   = subset_rows[0]["currency"] if subset_rows else ""

    # ── Merge slices < 3 % into "Other" to avoid label overlap ───────────────
    total = sum(raw_values)
    THRESHOLD = 0.03
    labels, values, other_val = [], [], 0.0
    for lbl, val in zip(raw_labels, raw_values):
        if total > 0 and val / total < THRESHOLD:
            other_val += val
        else:
            labels.append(lbl)
            values.append(val)
    if other_val > 0:
        labels.append("Other")
        values.append(other_val)

    colors = _PIE_PALETTE[:len(labels)]

    # ── Donut trace ──────────────────────────────────────────────────────────
    pie = go.Pie(
        labels=labels,
        values=values,
        hole=0.62,
        direction="clockwise",
        sort=True,
        textposition="inside",
        textinfo="none",          # hide all on-chart labels; legend handles it
        marker=dict(
            colors=colors,
            line=dict(color="#10141f", width=2),
        ),
        hovertemplate=(
            "<b>%{label}</b><br>"
            f"{currency} %{{value:,.2f}}<br>"
            "%{percent}<extra></extra>"
        ),
        pull=[0.02] * len(labels),
    )

    fig = go.Figure(pie)

    # ── Centre annotation ────────────────────────────────────────────────────
    total_fmt = f"{total:,.0f}"
    fig.add_annotation(
        text=f"<b>{total_fmt}</b><br><span style='font-size:11px;color:#6b7a99'>{currency}</span>",
        x=0.5, y=0.5,
        showarrow=False,
        font=dict(size=18, color="#dce3f0"),
        align="center",
        xref="paper", yref="paper",
    )

    # ── Legend (horizontal, bottom) ──────────────────────────────────────────
    fig.update_layout(
        title=dict(
            text=flag_title,
            font=dict(size=17, color="#dce3f0", family="sans-serif"),
            x=0.5, xanchor="center", y=0.97,
        ),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#c8d0e0", family="sans-serif"),
        legend=dict(
            orientation="h",
            yanchor="bottom", y=-0.22,
            xanchor="center", x=0.5,
            font=dict(size=12),
            bgcolor="rgba(0,0,0,0)",
            itemsizing="constant",
            traceorder="normal",
            itemclick=False,
            itemdoubleclick=False,
        ),
        margin=dict(t=48, b=80, l=20, r=20),
        height=420,
        showlegend=True,
    )
    return fig

_pie_items = []
if tw_rows:
    _pie_items.append(("pie_tw", tw_rows, "no_tw"))
if us_rows:
    _pie_items.append(("pie_us", us_rows, "no_us"))
if hk_rows:
    _pie_items.append(("pie_hk", hk_rows, "no_hk"))
if not _pie_items:
    _pie_items = [("pie_tw", [], "no_tw"), ("pie_us", [], "no_us")]
_pie_cols = st.columns(len(_pie_items))
for _pc, (_pk, _pr, _ek) in zip(_pie_cols, _pie_items):
    with _pc:
        if _pr:
            st.plotly_chart(_make_pie(_pr, t(_pk)), width='stretch', key=_pk)
        else:
            st.caption(t(_ek))

# -- Sector Distribution -------------------------------------------------------
st.markdown(f'<p class="section-title">{t("sec_sector")}</p>', unsafe_allow_html=True)

_SECTOR_COLORS = {
    "Technology": "#7eb8f7",
    "Financial Services": "#6ee7b7",
    "Healthcare": "#f9a8d4",
    "Consumer Cyclical": "#fcd34d",
    "Industrials": "#c4b5fd",
    "Communication Services": "#fdba74",
    "Consumer Defensive": "#86efac",
    "Energy": "#fca5a5",
    "Basic Materials": "#6edfd8",
    "Real Estate": "#a5b4fc",
    "Utilities": "#fde68a",
}


def _make_sector_pie(subset_rows, title):
    _unknown = t("sector_unknown")
    by_sector: dict = defaultdict(float)
    for r in subset_rows:
        sector = (_sd_info.get(r["sym"]) or {}).get("sector") or _unknown
        by_sector[sector] += r["mkt_val"]
    labels = list(by_sector.keys())
    values = list(by_sector.values())
    colors = [_SECTOR_COLORS.get(lb, "#e5e7eb") for lb in labels]
    currency = subset_rows[0]["currency"] if subset_rows else ""
    n_sectors = len(labels)
    fig = go.Figure(go.Pie(
        labels=labels, values=values, hole=0.56,
        direction="clockwise", sort=True,
        textposition="inside", textinfo="none",
        marker=dict(colors=colors, line=dict(color="#10141f", width=2)),
        hovertemplate=(
            "<b>%{label}</b><br>"
            + f"{currency} %{{value:,.0f}}<br>"
            + "%{percent}<extra></extra>"
        ),
        pull=[0.02] * n_sectors,
    ))
    fig.add_annotation(
        text=f"<b>{n_sectors}</b><br><span style='font-size:11px;color:#6b7a99'>{t('sector_label')}</span>",
        x=0.5, y=0.5, showarrow=False,
        font=dict(size=18, color="#dce3f0"), align="center",
        xref="paper", yref="paper",
    )
    fig.update_layout(
        title=dict(text=title, font=dict(size=17, color="#dce3f0", family="sans-serif"),
                   x=0.5, xanchor="center", y=0.97),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#c8d0e0", family="sans-serif"),
        legend=dict(orientation="h", yanchor="bottom", y=-0.22, xanchor="center", x=0.5,
                    font=dict(size=12), bgcolor="rgba(0,0,0,0)",
                    itemsizing="constant", itemclick=False, itemdoubleclick=False),
        margin=dict(t=48, b=80, l=20, r=20),
        height=420, showlegend=True,
    )
    return fig


_sec_items = []
if tw_rows:
    _sec_items.append(("sector_tw", tw_rows, "no_tw", "sector_pie_tw"))
if us_rows:
    _sec_items.append(("sector_us", us_rows, "no_us", "sector_pie_us"))
if hk_rows:
    _sec_items.append(("sector_hk", hk_rows, "no_hk", "sector_pie_hk"))
if not _sec_items:
    _sec_items = [("sector_tw", [], "no_tw", "sector_pie_tw"), ("sector_us", [], "no_us", "sector_pie_us")]
_sec_cols = st.columns(len(_sec_items))
for _sc, (_sk, _sr, _sek, _skey) in zip(_sec_cols, _sec_items):
    with _sc:
        if _sr:
            st.plotly_chart(_make_sector_pie(_sr, t(_sk)), width='stretch', key=_skey)
        else:
            st.caption(t(_sek))

# -- Dividend Records ----------------------------------------------------------
st.markdown(f'<p class="section-title">{t("sec_dividend")}</p>', unsafe_allow_html=True)


def _make_div_df(subset_rows):
    c_ticker = t("col_ticker")
    c_sector = t("div_col_sector")
    c_yield  = t("div_col_yield")
    c_rate   = t("div_col_annual_rate")
    c_income = t("div_col_annual_income")
    data = []
    for r in subset_rows:
        _sdi = _sd_info.get(r["sym"]) or {}
        _dy  = _sdi.get("dividend_yield")
        _dr  = _sdi.get("dividend_rate")
        data.append({
            c_ticker: r["sym"],
            c_sector: _sdi.get("sector") or "—",
            c_yield:  (_dy * 100) if _dy else float("nan"),
            c_rate:   _dr if _dr else float("nan"),
            c_income: (r["qty"] * _dr) if _dr else float("nan"),
        })
    df = pd.DataFrame(data)
    return (
        df.style
        .format({
            c_yield:  lambda x: f"{x:.2f}%" if not pd.isna(x) else "—",
            c_rate:   lambda x: f"{x:.4f}"  if not pd.isna(x) else "—",
            c_income: lambda x: f"{x:,.2f}" if not pd.isna(x) else "—",
        })
        .set_properties(subset=[c_yield, c_rate, c_income], **{"text-align": "right"})
        .set_properties(subset=[c_ticker], **{"font-weight": "700"})
    )


_div_items = []
if tw_rows:
    _div_items.append(("tw_stocks", tw_rows, "no_tw", "div_tw_total", "TWD"))
if us_rows:
    _div_items.append(("us_stocks", us_rows, "no_us", "div_us_total", "USD"))
if hk_rows:
    _div_items.append(("hk_stocks", hk_rows, "no_hk", "div_hk_total", "HKD"))
if not _div_items:
    _div_items = [("tw_stocks", [], "no_tw", "div_tw_total", "TWD"), ("us_stocks", [], "no_us", "div_us_total", "USD")]
_div_cols = st.columns(len(_div_items))
for _divc, (_dlbl, _drows, _dek, _dtot_key, _dccy) in zip(_div_cols, _div_items):
    with _divc:
        st.markdown(f"**{t(_dlbl)}**")
        if _drows:
            st.dataframe(_make_div_df(_drows), width='stretch', hide_index=True)
            _ann_div = sum(
                r["qty"] * ((_sd_info.get(r["sym"]) or {}).get("dividend_rate") or 0)
                for r in _drows
            )
            if _ann_div > 0:
                st.caption(f"📌 {t(_dtot_key)}: {_dccy} {_ann_div:,.2f}")
        else:
            st.caption(t(_dek))

# -- Portfolio Performance Chart ----------------------------------------------
st.markdown(f'<p class="section-title">{t("sec_perf")}</p>', unsafe_allow_html=True)

_perf_period_opts = t("perf_period_opts")
_perf_period_vals = t("perf_period_vals")
_pp_col, _ = st.columns([2, 6])
with _pp_col:
    _perf_idx = st.selectbox(t("perf_period_label"), range(len(_perf_period_opts)),
                             format_func=lambda i: _perf_period_opts[i],
                             index=1, key="perf_period")
_perf_str  = _perf_period_vals[_perf_idx]
_holdings_json = json.dumps(sorted(
    [{"sym": r["sym"], "qty": r["qty"], "cost": r["cost"], "currency": r["currency"]}
     for r in rows],
    key=lambda x: x["sym"]
))
with st.spinner(t("perf_fetching")):
    _perf_data = _fetch_portfolio_history(_holdings_json, _perf_str)

if not _perf_data:
    st.caption(t("perf_no_data"))
else:
    _ccy_labels = {"TWD": t("perf_tw_label"), "USD": t("perf_us_label"), "HKD": t("perf_hk_label")}
    _perf_cols = st.columns(len(_perf_data))
    for _pidx, (_ccy, _pdata) in enumerate(_perf_data.items()):
        with _perf_cols[_pidx]:
            _pdates     = pd.to_datetime(_pdata["dates"])
            _pvalues    = _pdata["values"]
            _total_cost = _pdata["total_cost"]
            _curr_val   = _pvalues[-1] if _pvalues else 0
            _ppnl       = _curr_val - _total_cost if _total_cost > 0 else None
            _ppnl_pct   = (_ppnl / _total_cost * 100) if (_ppnl is not None and _total_cost > 0) else None
            _plabel     = _ccy_labels.get(_ccy, _ccy)

            _clr_line   = "#7eb8f7" if _ccy == "USD" else "#6ee7b7"
            _clr_fill   = "rgba(126,184,247,0.10)" if _ccy == "USD" else "rgba(110,231,183,0.10)"

            _pfig = go.Figure()
            _pfig.add_trace(go.Scatter(
                x=_pdates, y=_pvalues, name=_plabel,
                fill="tozeroy", fillcolor=_clr_fill,
                line=dict(color=_clr_line, width=2),
                hovertemplate=f"{_ccy} %{{y:,.0f}}<extra></extra>",
            ))
            if _total_cost > 0:
                _pfig.add_hline(
                    y=_total_cost,
                    line=dict(color="rgba(253,211,77,0.55)", width=1.5, dash="dash"),
                    annotation_text=f"{t('perf_cost')} {_total_cost:,.0f}",
                    annotation_font=dict(color="#fcd34d", size=10),
                    annotation_position="top left",
                )
            _delta_html = ""
            if _ppnl is not None:
                _parrow = "▲" if _ppnl >= 0 else "▼"
                _pcolor = "#6ee7b7" if _ppnl >= 0 else "#fca5a5"
                _pct_s  = f"{_parrow} {_ppnl_pct:+.2f}%" if _ppnl_pct else ""
                _delta_html = f' <span style="color:{_pcolor};font-size:13px;font-weight:600">{_pct_s}</span>'
            st.markdown(f"**{_plabel}** — {_ccy} {_curr_val:,.0f}{_delta_html}", unsafe_allow_html=True)
            _pfig.update_layout(
                height=220, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="#0e1117",
                font=dict(color="#c8d0e0", size=11),
                margin=dict(t=8, b=8, l=8, r=8),
                showlegend=False, xaxis_rangeslider_visible=False,
                yaxis=dict(tickformat=",.0f"),
            )
            _pfig.update_xaxes(gridcolor="rgba(255,255,255,0.04)")
            _pfig.update_yaxes(gridcolor="rgba(255,255,255,0.04)")
            st.plotly_chart(_pfig, width='stretch', key=f"perf_{_ccy}")

# -- Transaction History -------------------------------------------------------
st.markdown(f'<p class="section-title">{t("trans_history")}</p>', unsafe_allow_html=True)
if "cached_txns" not in st.session_state:
    st.session_state.cached_txns = load_transactions(st.session_state.user_id)
_txns = st.session_state.cached_txns
if not _txns:
    st.caption(t("trans_no_records"))
else:
    # Build HTML table (much cleaner than st.columns per row)
    _rows_html = ""
    for _txn_id, _tsym, _tdate, _tqty, _tprice, _ttype in _txns:
        _badge = f'<span class="badge-buy">{t("trans_buy_label")}</span>' \
                 if _ttype == "buy" else \
                 f'<span class="badge-sell">{t("trans_sell_label")}</span>'
        _rows_html += f"""
<tr>
  <td>{_tdate}</td>
  <td><strong>{_tsym}</strong></td>
  <td>{_badge}</td>
  <td style="text-align:right">{_tqty:g}</td>
  <td style="text-align:right">{_tprice:.2f}</td>
  <td style="text-align:right;color:#4a5568;font-size:11px" data-id="{_txn_id}">#{_txn_id}</td>
</tr>"""
    st.markdown(f"""
<div style="background:#161c2d;border:1px solid rgba(255,255,255,0.06);border-radius:12px;overflow:hidden;margin-bottom:8px">
<table class="txn-table">
<thead><tr>
  <th>{t("trans_col_date")}</th>
  <th>{t("trans_col_ticker")}</th>
  <th>{t("trans_col_type")}</th>
  <th style="text-align:right">{t("trans_col_qty")}</th>
  <th style="text-align:right">{t("trans_col_price")}</th>
  <th style="text-align:right">ID</th>
</tr></thead>
<tbody>{_rows_html}</tbody>
</table>
</div>""", unsafe_allow_html=True)

    # Delete control below the table
    with st.expander(f"🗑️ {t('trans_delete_help')}", expanded=False):
        _del_options = {f"#{tid} | {tsym} {tdate} {ttype} {tqty:g}@{tprice:.2f}": tid
                        for tid, tsym, tdate, tqty, tprice, ttype in _txns}
        _del_sel = st.selectbox("", list(_del_options.keys()), label_visibility="collapsed",
                                key="del_txn_select")
        if st.button(f"🗑️ {t('trans_delete_help')}", key="del_txn_btn"):
            _affected = delete_transaction(_del_options[_del_sel], st.session_state.user_id)
            if _affected:
                st.session_state.portfolio = load_holdings(st.session_state.user_id)
                st.session_state.cached_news = {}
                st.session_state.news_portfolio_key = ""
                st.session_state.pop("cached_txns", None)
                st.success(t("trans_deleted"))
                st.rerun()

# -- News ---------------------------------------------------------------------
st.markdown(f'<p class="section-title">{t("sec_news")}</p>', unsafe_allow_html=True)

def _thumb_html(article):
    """Return an <img> tag if thumbnail available, else a placeholder div."""
    try:
        resolutions = article["thumbnail"]["resolutions"]
        url = next(
            (r["url"] for r in resolutions if r.get("width", 0) >= 200),
            resolutions[0]["url"],
        )
        url = html_escape(url, quote=True)
        if not url.startswith(("http://", "https://")):
            raise ValueError
        return f'<img class="gn-thumb" src="{url}" alt="" loading="lazy">'
    except Exception:
        return '<div class="gn-thumb-placeholder">📄</div>'

def _render_news_tabs(syms, cached):
    """Render Google-News style tabs, one per ticker, cards in a 2-col grid."""
    valid_syms = [s for s in syms if cached.get(s)]
    if not valid_syms:
        st.caption(t("no_news_tab"))
        return
    tabs = st.tabs(valid_syms)
    for tab, sym in zip(tabs, valid_syms):
        with tab:
            articles = cached[sym]
            cards_html = ""
            for article in articles:
                title  = html_escape(article.get("title") or "(no title)")
                link   = html_escape(article.get("link") or "#", quote=True)
                if not link.startswith(("http://", "https://")):
                    link = "#"
                pub    = html_escape(article.get("publisher") or "")
                ts     = article.get("providerPublishTime")
                dt_str = datetime.fromtimestamp(ts, tz=timezone(timedelta(hours=8))).strftime("%m/%d %H:%M") if ts else ""
                thumb  = _thumb_html(article)
                cards_html += f"""
<div class="gn-card">
    {thumb}
    <div class="gn-body">
        <div class="gn-title"><a href="{link}" target="_blank">{title}</a></div>
        <div class="gn-meta">
            <span class="gn-source">{pub}</span>
            <span class="gn-time">{dt_str}</span>
        </div>
    </div>
</div>"""
            st.markdown(f'<div class="gn-grid">{cards_html}</div>', unsafe_allow_html=True)

if not st.session_state.cached_news:
    st.warning(t("news_not_found"))
else:
    _news_items = []
    if tw_rows:
        _news_items.append(("news_tw", [r["sym"] for r in tw_rows]))
    if us_rows:
        _news_items.append(("news_us", [r["sym"] for r in us_rows]))
    if hk_rows:
        _news_items.append(("news_hk", [r["sym"] for r in hk_rows]))
    if not _news_items:
        _news_items = [("news_tw", []), ("news_us", [])]
    _news_cols = st.columns(len(_news_items))
    for _nc, (_nk, _nsyms) in zip(_news_cols, _news_items):
        with _nc:
            st.markdown(t(_nk))
            _render_news_tabs(_nsyms, st.session_state.cached_news)

# -- Footer -------------------------------------------------------------------
st.divider()
col_time, col_btn = st.columns([3, 1])
col_time.caption(
    t("footer_time").format(
        datetime.now().strftime("%H:%M:%S"),
        st.session_state.refresh_interval,
    )
)
if col_btn.button(t("refresh_now")):
    st.rerun()