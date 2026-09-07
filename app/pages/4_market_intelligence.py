"""
Page 5: Market Intelligence
Analytical story: "Pattern và signal nào đang xuất hiện?"

Sections:
  1. BTC Dominance Regime (Insight #2) — trend + current signal
  2. Market Concentration over time (top10_market_share_pct)
  3. Active Cryptocurrencies trend (nếu có)
  4. Data Pipeline Health
"""

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from datetime import UTC, datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from app import charts, theme
from app.queries import get_market_health_history, get_market_overview, get_top_movers

st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
[data-testid="metric-container"] {
    background-color: #161B22; border: 1px solid #30363D;
    border-radius: 10px; padding: 12px 16px;
}
[data-testid="metric-container"] label { font-size: 11px !important; color: #8B949E !important; font-weight: 500; text-transform: uppercase; letter-spacing: 0.05em; }
[data-testid="metric-container"] [data-testid="stMetricValue"] { font-size: 16px !important; font-weight: 700; color: #E6EDF3 !important; }
[data-testid="stMetricDelta"] { font-size: 12px !important; font-weight: 600; }
section[data-testid="stSidebar"] { background-color: #161B22; border-right: 1px solid #30363D; }
</style>
""",
    unsafe_allow_html=True,
)

with st.sidebar:
    st.markdown("## 📊 Crypto Dashboard")
    st.markdown("---")
    st.caption("Data: CoinGecko via pipeline")

# ─── Header ───────────────────────────────────────────────────────────────
st.markdown("# 🧠 Market Intelligence")
st.markdown("*Market regime signals, concentration trends, và pipeline health*")
st.markdown("---")

history = get_market_health_history()
overview = get_market_overview()

if overview.empty:
    st.error("⚠️ Chưa có dữ liệu.")
    st.stop()

row = overview.iloc[0]

# ─── Section 1: BTC Dominance Regime (Insight #2) ─────────────────────────
st.subheader("🟠 BTC Dominance & Market Regime — Insight #2")

with st.expander("ℹ️ BTC Dominance Regime là gì?"):
    st.markdown("""
**BTC Dominance** đo tỷ lệ % market cap của Bitcoin trong tổng thị trường.

| BTC Dominance | Regime | Ý nghĩa |
|--------------|--------|---------|
| > 55% | 🟠 Bitcoin Season | Nhà đầu tư tập trung vào BTC, altcoins underperform |
| 45–55% | ⚪ Neutral | Cân bằng — không có xu hướng rõ ràng |
| < 45% | 🟣 Altcoin Season | Tiền chảy từ BTC sang altcoins |

**Tại sao quan trọng?** Biết đang ở giai đoạn nào giúp quyết định phân bổ capital hợp lý hơn.

> ⚠️ Đây là **observation** từ data, không phải investment advice.
""")

btc_dom_now = row["btc_dominance_pct"]
eth_dom_now = row["eth_dominance_pct"]

# Regime determination
if btc_dom_now >= 55:
    regime = "🟠 Bitcoin Season"
    regime_color = theme.COIN_COLORS["bitcoin"]
    regime_detail = (
        f"BTC chiếm **{btc_dom_now:.1f}%** thị trường. "
        "Trong giai đoạn này, altcoins thường underperform so với BTC. "
        "Investors đang 'flight to BTC safety' hoặc BTC đang trong uptrend mạnh."
    )
elif btc_dom_now <= 45:
    regime = "🟣 Altcoin Season"
    regime_color = theme.COIN_COLORS["ethereum"]
    regime_detail = (
        f"BTC dominance thấp ({btc_dom_now:.1f}%). "
        "Tiền đang chảy từ BTC sang altcoins. "
        "Thường xảy ra khi BTC đã accumulate đủ và investors tìm kiếm return cao hơn từ altcoins."
    )
else:
    regime = "⚪ Neutral Market"
    regime_color = theme.NEUTRAL_GRAY
    regime_detail = (
        f"BTC dominance ở mức trung tính ({btc_dom_now:.1f}%). "
        "Không có xu hướng rõ ràng — thị trường đang trong giai đoạn chuyển tiếp hoặc sideways."
    )

st.markdown(
    f"<div style='background: #161B22; border: 1px solid {regime_color}; border-left: 4px solid {regime_color}; "
    f"border-radius: 8px; padding: 16px 20px; margin-bottom: 16px;'>"
    f"<h3 style='color:{regime_color}; margin: 0 0 8px 0'>{regime}</h3>"
    f"<p style='color: #E6EDF3; margin: 0'>{regime_detail}</p>"
    f"</div>",
    unsafe_allow_html=True,
)

c1, c2, c3 = st.columns(3)
c1.metric("BTC Dominance", f"{btc_dom_now:.2f}%")
c2.metric("ETH Dominance", f"{eth_dom_now:.2f}%")
c3.metric(
    "Others",
    f"{100 - btc_dom_now - eth_dom_now:.2f}%",
    help="100% - BTC - ETH. Phần còn lại là altcoins.",
)

# Dominance chart
if history.shape[0] >= 2:
    fig_dom = charts.dominance_area(history)
    st.plotly_chart(fig_dom, use_container_width=True, key="intel_dominance")
else:
    st.info("📊 Dominance trend chart sẽ hiển thị khi có ≥2 snapshots.")

st.markdown("---")

# ─── Section 2: Market Concentration ─────────────────────────────────────
st.subheader("🏦 Market Concentration — Top 10 Coins")

with st.expander("ℹ️ Market Concentration là gì?"):
    st.markdown("""
**Top 10 Market Share** = Tổng market cap của 10 coin pipeline đang track / Tổng market cap toàn thị trường.

- **Cao (>85%)**: Thị trường tập trung — top coins chi phối; altcoins nhỏ ít ảnh hưởng
- **Thấp (<70%)**: Thị trường phân tán — nhiều altcoins nhỏ đang nhận đầu tư

**Pipeline tracking**: Bitcoin, Ethereum, Tether, USD Coin, BNB, Solana, XRP, Dogecoin, Cardano, Shiba Inu
""")

top10_share = row.get("top10_market_share_pct")
top10_mc = row.get("top10_market_cap_usd")
total_mc = row.get("total_market_cap_usd")

c1c, c2c = st.columns(2)
c1c.metric(
    "Top 10 Market Share",
    f"{top10_share * 100:.2f}%" if pd.notna(top10_share) else "N/A",
    help="Tỷ lệ % market cap của 10 coin pipeline track so với toàn thị trường",
)
c2c.metric(
    "Top 10 Market Cap",
    f"${top10_mc / 1e12:.3f}T"
    if pd.notna(top10_mc) and top10_mc >= 1e12
    else (f"${top10_mc / 1e9:.1f}B" if pd.notna(top10_mc) else "N/A"),
)

# Concentration over time
if history.shape[0] >= 2 and "top10_market_share_pct" in history.columns:
    fig_conc = go.Figure(
        go.Scatter(
            x=history["fetched_at"],
            y=history["top10_market_share_pct"] * 100,
            mode="lines+markers",
            line=dict(color=theme.ACCENT_PURPLE, width=2),
            marker=dict(size=5),
            fill="tozeroy",
            fillcolor="rgba(124,77,255,0.1)",
            name="Top 10 Share %",
            hovertemplate="<b>%{x}</b><br>Top 10 Share: %{y:.2f}%<extra></extra>",
        )
    )
    fig_conc.add_hline(
        y=80,
        line_dash="dash",
        line_color=theme.NEUTRAL_GRAY,
        annotation_text="80% reference",
        annotation_font_size=10,
    )
    fig_conc.update_layout(
        template=theme.CHART_TEMPLATE,
        paper_bgcolor=theme.CHART_PAPER_BG,
        plot_bgcolor=theme.CHART_PLOT_BG,
        font=dict(color=theme.CHART_FONT_COLOR, family="Inter, sans-serif", size=12),
        title=dict(
            text="Top 10 Market Concentration (%)", font=dict(size=14, color=theme.TEXT_PRIMARY)
        ),
        margin=theme.CHART_MARGIN,
        height=300,
        xaxis=dict(gridcolor=theme.BORDER, showgrid=True),
        yaxis=dict(gridcolor=theme.BORDER, showgrid=True, title="Share %"),
    )
    st.plotly_chart(fig_conc, use_container_width=True, key="concentration")
else:
    st.info("📊 Concentration trend sẽ xuất hiện khi có ≥2 snapshots.")

st.markdown("---")

# ─── Section 3: Pipeline Health & Data Freshness ─────────────────────────
st.subheader("⚙️ Pipeline Data Health")

fetched_at = row["fetched_at"]

now_utc = datetime.now(UTC).replace(tzinfo=None)
staleness_min = (now_utc - fetched_at.to_pydatetime().replace(tzinfo=None)).total_seconds() / 60

if staleness_min < 120:
    freshness_icon = "🟢"
    freshness_label = "Fresh"
elif staleness_min < 360:
    freshness_icon = "🟡"
    freshness_label = "Stale"
else:
    freshness_icon = "🔴"
    freshness_label = "Very Stale"

c1d, c2d, c3d = st.columns(3)
c1d.metric("Last Pipeline Run", fetched_at.strftime("%Y-%m-%d %H:%M") + " UTC")
c2d.metric(
    "Data Freshness", f"{freshness_icon} {freshness_label}", delta=f"{staleness_min:.0f} min ago"
)
c3d.metric(
    "Snapshots in History", str(len(history)), help="Tổng số lần pipeline fetch global endpoint"
)

# Summary table
st.markdown("**Data Volume Summary**")
summary_data = {
    "Table": ["market_health_mart", "top_movers_mart", "coin_performance_mart"],
    "Grain": ["1 row / fetch global", "1 row / coin (latest)", "1 row / coin / day"],
    "Snapshots": [
        f"{len(history)} fetches",
        f"{len(get_top_movers())} coins",
        "—",
    ],
}
st.dataframe(pd.DataFrame(summary_data), use_container_width=True, hide_index=True)

st.markdown("---")
st.markdown(
    "<p style='color: #6E7681; font-size: 12px; text-align: center'>"
    "Data source: CoinGecko API · Pipeline: Airflow DAG · Warehouse: DuckDB · Transform: dbt"
    "</p>",
    unsafe_allow_html=True,
)
