"""
Page 1: Market Overview (Home)
Analytical story: "Thị trường hôm nay đang ở đâu?"

Flow:
  1. KPI strip — snapshot mới nhất (market cap, volume, BTC dominance, top10 share)
  2. Market Cap & 24h Change trend
  3. BTC / ETH Dominance area chart
  4. Data freshness footer
"""

import sys
from pathlib import Path

# Đảm bảo project root trong sys.path khi chạy bằng `streamlit run app/main.py`
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import streamlit as st
import pandas as pd

from app.queries import get_market_overview, get_market_health_history
from app import charts, theme

# ─── Page config ──────────────────────────────────────────────────────────

# ─── Global CSS ───────────────────────────────────────────────────────────
st.markdown("""
<style>
/* Import Inter from Google Fonts */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

/* KPI metric cards */
[data-testid="metric-container"] {
    background-color: #161B22;
    border: 1px solid #30363D;
    border-radius: 10px;
    padding: 12px 16px;
}
[data-testid="metric-container"] label {
    font-size: 11px !important;
    color: #8B949E !important;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-size: 16px !important;
    font-weight: 700;
    color: #E6EDF3 !important;
}
[data-testid="stMetricDelta"] {
    font-size: 12px !important;
    font-weight: 600;
}

/* Sidebar header */
section[data-testid="stSidebar"] {
    background-color: #161B22;
    border-right: 1px solid #30363D;
}

/* Dividers */
hr {
    border-color: #30363D;
    margin: 12px 0;
}

/* Streamlit plotly charts – remove default white padding */
.js-plotly-plot { border-radius: 8px; }

/* Info/warning boxes */
.stAlert {
    border-radius: 8px;
    border: 1px solid #30363D;
}
</style>
""", unsafe_allow_html=True)


# ─── Sidebar navigation label ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📊 Crypto Dashboard")
    st.markdown("---")
    st.markdown("**Navigate**")
    st.markdown(
        "Use the **pages** in the sidebar to explore:\n"
        "- 🏠 **Overview** — Market macro\n"
        "- 🚀 **Top Movers** — Gainers & Losers\n"
        "- 🔍 **Coin Deep Dive** — Single-coin analysis\n"
        "- ⚖️ **Comparison** — Multi-coin\n"
        "- 🧠 **Intelligence** — Market signals\n"
    )
    st.markdown("---")
    st.caption("Data: CoinGecko API via pipeline")


# ─── Header ───────────────────────────────────────────────────────────────
st.markdown("# 🏠 Market Overview")
st.markdown("*Snapshot tổng quan thị trường crypto — macro indicators*")
st.markdown("---")

# ─── Load data ────────────────────────────────────────────────────────────
overview = get_market_overview()

if overview.empty:
    st.error(
        "⚠️ Chưa có dữ liệu trong `market_health_mart`.\n\n"
        "Hãy chắc chắn rằng pipeline (ingest → dbt transform) đã chạy ít nhất một lần."
    )
    st.stop()

row = overview.iloc[0]

# ─── KPI Strip ────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)

# Market Cap
total_mc = row["total_market_cap_usd"]
mc_change = row.get("market_cap_change_pct_24h")
mc_delta = f"{mc_change:+.2f}%" if pd.notna(mc_change) else None
c1.metric(
    "Total Market Cap",
    f"${total_mc / 1e12:.3f}T",
    delta=mc_delta,
    help="Tổng vốn hóa toàn thị trường crypto (USD). Delta = % thay đổi 24h từ CoinGecko API.",
)

# 24h Volume
total_vol = row["total_volume_usd"]
vol_change = row.get("volume_change_pct_24h")
vol_delta = f"{vol_change:+.2f}%" if pd.notna(vol_change) else None
c2.metric(
    "24h Volume",
    f"${total_vol / 1e9:.1f}B",
    delta=vol_delta,
    help="Tổng khối lượng giao dịch 24h toàn thị trường (USD).",
)

# BTC Dominance
btc_dom = row["btc_dominance_pct"]
c3.metric(
    "BTC Dominance",
    f"{btc_dom:.2f}%",
    delta=None,
    help=(
        "Thị phần market cap của Bitcoin so với toàn thị trường. "
        ">50% = Bitcoin season (tiền chảy về BTC). "
        "<45% = Altcoin season (tiền chảy sang altcoins)."
    ),
)

# Top 10 Concentration
top10_share = row.get("top10_market_share_pct")
top10_pct = top10_share * 100 if pd.notna(top10_share) else None
c4.metric(
    "Top 10 Concentration",
    f"{top10_pct:.1f}%" if top10_pct is not None else "N/A",
    delta=None,
    help=(
        "Tỷ lệ % market cap của top 10 coin so với toàn thị trường. "
        "Thường dao động 70-90%. Giảm = thị trường phân tán hơn."
    ),
)

# Data freshness
fetched = row["fetched_at"]
st.caption(f"🕐 Snapshot: **{fetched}** UTC · Pipeline data delay ~1-4h")
st.markdown("---")

# ─── Market Regime Indicator ──────────────────────────────────────────────
btc_dom_val = row["btc_dominance_pct"]
mc_chg_val  = row.get("market_cap_change_pct_24h", 0) or 0

if btc_dom_val >= 55:
    regime_label = "🟠 Bitcoin Season"
    regime_desc  = (
        f"BTC dominance cao ({btc_dom_val:.1f}%) — thị trường đang tập trung vào Bitcoin. "
        "Altcoins thường underperform trong giai đoạn này."
    )
elif btc_dom_val <= 45:
    regime_label = "🟣 Altcoin Season"
    regime_desc  = (
        f"BTC dominance thấp ({btc_dom_val:.1f}%) — tiền đang chảy sang altcoins. "
        "Thường xảy ra sau khi BTC đã tăng mạnh."
    )
else:
    regime_label = "⚪ Neutral"
    regime_desc  = (
        f"BTC dominance ở mức trung tính ({btc_dom_val:.1f}%). "
        "Không có tín hiệu rõ ràng về Bitcoin hay Altcoin season."
    )

if mc_chg_val > 2:
    regime_desc += f"  |  📈 Thị trường đang **tăng** (+{mc_chg_val:.2f}% 24h)"
elif mc_chg_val < -2:
    regime_desc += f"  |  📉 Thị trường đang **giảm** ({mc_chg_val:.2f}% 24h)"

st.info(f"**Market Regime: {regime_label}**\n\n{regime_desc}")

# ─── Historical Trend Charts ─────────────────────────────────────────────
history = get_market_health_history()

if history.shape[0] >= 2:
    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.subheader("Market Cap Trend")
        fig_mc = charts.market_cap_trend(history)
        st.plotly_chart(fig_mc, use_container_width=True, key="mc_trend")

    with col_right:
        st.subheader("BTC & ETH Dominance")
        fig_dom = charts.dominance_area(history)
        st.plotly_chart(fig_dom, use_container_width=True, key="dominance")
else:
    st.info(
        "📊 Trend charts sẽ xuất hiện khi pipeline có **≥2 data points** "
        f"(hiện tại: {history.shape[0]} snapshot(s)). Hãy để pipeline chạy thêm."
    )

# ─── ETH Dominance note ──────────────────────────────────────────────────
eth_dom = row["eth_dominance_pct"]
with st.expander("ℹ️ Đọc thêm: BTC Dominance & Altcoin Season"):
    st.markdown(f"""
**BTC Dominance hiện tại: {btc_dom_val:.2f}%** | **ETH Dominance: {eth_dom:.2f}%**

| Range | Ý nghĩa |
|-------|---------|
| BTC Dom > 55% | Bitcoin Season — investors ưu tiên BTC, altcoins underperform |
| BTC Dom 45–55% | Neutral — thị trường cân bằng |
| BTC Dom < 45% | Altcoin Season — tiền chảy sang altcoins |

> 💡 **Lưu ý**: BTC + ETH dominance = {btc_dom_val + eth_dom:.1f}%. Phần còn lại ({100 - btc_dom_val - eth_dom:.1f}%) thuộc các altcoins khác.
""")
