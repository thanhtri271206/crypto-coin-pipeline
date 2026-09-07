"""
Page 2: Top Movers
Analytical story: "Coin nào đang thắng/thua? Volume bất thường ở đâu?"

Sections:
  1. Gainers / Losers tabs (24h và 7d) — bar chart + table
  2. Volume activity overview — table với spike ratio
  3. Scatter: Volume Spike vs. Price Change (Insight #1)
  4. Market rank snapshot table

Stablecoin note: USDT và USDC được hiển thị trong bảng market rank
nhưng được filter ra khỏi gainers/losers chart vì price change ~0% không mang thông tin.
"""

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pandas as pd
import streamlit as st

from app import charts, theme
from app.queries import get_top_movers

# ─── Shared CSS ──────────────────────────────────────────────
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
st.markdown("# 🚀 Top Movers")
st.markdown("*Snapshot mới nhất — Gainers, Losers, và Volume Anomalies*")
st.markdown("---")

# ─── Load data ────────────────────────────────────────────────────────────
df_raw = get_top_movers()

if df_raw.empty:
    st.error("⚠️ Chưa có dữ liệu. Pipeline chưa chạy hoặc top_movers_mart rỗng.")
    st.stop()

# Chuẩn bị display names
df_raw["display_name"] = df_raw["coin_id"].map(theme.COIN_NAMES).fillna(df_raw["coin_id"])
df_raw["symbol"] = df_raw["coin_id"].map(theme.COIN_SYMBOLS).fillna(df_raw["coin_id"].str.upper())

# Filter stablecoins cho gainers/losers
df_no_stable = df_raw[~df_raw["coin_id"].isin(theme.STABLECOINS)].copy()

# ─── KPI row ──────────────────────────────────────────────────────────────
latest_time = df_raw["fetched_at"].max()
n_coins = len(df_raw)
n_gainers = (df_no_stable["price_change_percentage_24h"] > 0).sum()
n_losers = (df_no_stable["price_change_percentage_24h"] < 0).sum()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Coins Tracked", str(n_coins))
c2.metric("Gainers (24h)", str(n_gainers), help="Non-stablecoin coins với price_change_24h > 0")
c3.metric("Losers (24h)", str(n_losers), help="Non-stablecoin coins với price_change_24h < 0")
c4.metric("Data As Of", latest_time.strftime("%H:%M UTC"), help=f"Pipeline snapshot: {latest_time}")
st.markdown("---")

# ─── Section 1: Gainers / Losers ──────────────────────────────────────────
st.subheader("📈 Gainers & Losers")
st.caption(
    "ℹ️ **Stablecoins** (Tether/USDT và USD Coin/USDC) được loại ra khỏi bảng này vì chúng được thiết kế "
    "để giữ giá cố định ~$1 USD — price change luôn ~0%, không phản ánh biến động thị trường."
)

tab24h, tab7d = st.tabs(["24h Change", "7-Day Change"])

with tab24h:
    col_chart, col_table = st.columns([2, 3])

    with col_chart:
        fig = charts.gainers_losers_bar(
            df_no_stable, "price_change_percentage_24h", "Price Change — 24h (%)"
        )
        st.plotly_chart(fig, use_container_width=True, key="bar_24h")

    with col_table:
        df_24h = df_no_stable[
            [
                "symbol",
                "current_price",
                "price_change_percentage_24h",
                "total_volume",
                "market_cap_rank",
                "rank_change",
            ]
        ].copy()
        df_24h = df_24h.sort_values("price_change_percentage_24h", ascending=False)
        df_24h.columns = ["Symbol", "Price (USD)", "Change 24h %", "Volume 24h", "Rank", "Rank Δ"]

        def style_change(val):
            if pd.isna(val):
                return ""
            return f"color: {'#26A69A' if val >= 0 else '#EF5350'}; font-weight: 600"

        def style_rank(val):
            if pd.isna(val) or val == 0:
                return "color: #8B949E"
            return f"color: {'#26A69A' if val > 0 else '#EF5350'}; font-weight: 600"

        styled = (
            df_24h.style.map(style_change, subset=["Change 24h %"])
            .map(style_rank, subset=["Rank Δ"])
            .format(
                {
                    "Price (USD)": lambda x: f"${x:,.4f}" if x < 1 else f"${x:,.2f}",
                    "Change 24h %": lambda x: f"{x:+.2f}%" if pd.notna(x) else "N/A",
                    "Volume 24h": lambda x: f"${x / 1e9:.2f}B" if x >= 1e9 else f"${x / 1e6:.1f}M",
                    "Rank Δ": lambda x: f"{int(x):+d}" if pd.notna(x) else "N/A",
                }
            )
        )
        st.dataframe(styled, use_container_width=True, hide_index=True, height=320)

with tab7d:
    col_chart7, col_table7 = st.columns([2, 3])

    df_7d_avail = df_no_stable.dropna(subset=["price_change_percentage_7d_in_currency"])

    with col_chart7:
        if df_7d_avail.empty:
            st.info("7-day change data chưa có (cần đủ lịch sử từ CoinGecko API)")
        else:
            fig7 = charts.gainers_losers_bar(
                df_7d_avail, "price_change_percentage_7d_in_currency", "Price Change — 7 Days (%)"
            )
            st.plotly_chart(fig7, use_container_width=True, key="bar_7d")

    with col_table7:
        df_t7 = df_no_stable[
            ["symbol", "current_price", "price_change_percentage_7d_in_currency", "market_cap_rank"]
        ].copy()
        df_t7 = df_t7.sort_values("price_change_percentage_7d_in_currency", ascending=False)
        df_t7.columns = ["Symbol", "Price (USD)", "Change 7d %", "Rank"]
        styled7 = df_t7.style.map(style_change, subset=["Change 7d %"]).format(
            {
                "Price (USD)": lambda x: f"${x:,.4f}" if x < 1 else f"${x:,.2f}",
                "Change 7d %": lambda x: f"{x:+.2f}%" if pd.notna(x) else "N/A",
            }
        )
        st.dataframe(styled7, use_container_width=True, hide_index=True, height=320)

st.markdown("---")

# ─── Section 2: Volume Activity ───────────────────────────────────────────
st.subheader("📊 Volume Activity")

has_spike = df_raw["volume_spike_ratio"].notna().any()

if not has_spike:
    st.info(
        "⏳ **Volume Spike Ratio** sẽ có sau khi pipeline chạy ≥2 ngày. \n\n"
        "Volume bình thường hiện tại (không có baseline để so sánh):"
    )

col_vol1, col_vol2 = st.columns([3, 2])

with col_vol1:
    df_vol = df_raw[
        [
            "symbol",
            "display_name",
            "total_volume",
            "avg_volume_7d",
            "volume_spike_ratio",
            "price_change_percentage_24h",
        ]
    ].copy()
    df_vol = df_vol.sort_values("total_volume", ascending=False)
    df_vol.columns = ["Symbol", "Coin", "Volume 24h", "Avg Vol 7d", "Spike Ratio", "Price Δ 24h %"]

    def style_spike(val):
        if pd.isna(val):
            return "color: #6E7681"
        if val >= 3.0:
            return "color: #FFA726; font-weight: 700"
        if val >= 2.0:
            return "color: #FFA726"
        return "color: #8B949E"

    styled_vol = (
        df_vol.style.map(style_spike, subset=["Spike Ratio"])
        .map(style_change, subset=["Price Δ 24h %"])
        .format(
            {
                "Volume 24h": lambda x: f"${x / 1e9:.2f}B" if x >= 1e9 else f"${x / 1e6:.1f}M",
                "Avg Vol 7d": lambda x: (
                    f"${x / 1e9:.2f}B"
                    if pd.notna(x) and x >= 1e9
                    else (f"${x / 1e6:.1f}M" if pd.notna(x) else "N/A")
                ),
                "Spike Ratio": lambda x: f"{x:.2f}×" if pd.notna(x) else "N/A",
                "Price Δ 24h %": lambda x: f"{x:+.2f}%" if pd.notna(x) else "N/A",
            }
        )
    )
    st.dataframe(styled_vol, use_container_width=True, hide_index=True)

with col_vol2:
    if has_spike:
        with st.expander("📖 Đọc Volume Spike"):
            st.markdown("""
**Spike Ratio = Volume hôm nay / Avg Volume 7 ngày trước**

| Spike Ratio | Tín hiệu |
|-------------|----------|
| < 1.0× | Volume thấp hơn bình thường |
| 1.0–2.0× | Bình thường |
| 2.0–3.0× | ⚠️ Volume cao — có sự kiện |
| > 3.0× | 🔥 Volume bất thường |

**Kết hợp với price:**
- Spike + giá tăng → Breakout bullish
- Spike + giá đứng → Accumulation
- Spike + giá giảm → Capitulation/Distribution
""")
    else:
        st.markdown("""
**Volume hiện tại (top 3)**:

""")
        top3 = df_raw.nlargest(3, "total_volume")[["symbol", "total_volume"]]
        for _, r in top3.iterrows():
            vol = r["total_volume"]
            vol_str = f"${vol / 1e9:.2f}B" if vol >= 1e9 else f"${vol / 1e6:.1f}M"
            st.markdown(f"- **{r['symbol'].upper()}**: {vol_str}")

st.markdown("---")

# ─── Section 3: Volume Spike Scatter (Insight #1) ─────────────────────────
st.subheader("🔬 Volume Spike vs. Price Change — Insight #1")

if has_spike:
    fig_scatter = charts.volume_spike_scatter(df_no_stable)
    st.plotly_chart(fig_scatter, use_container_width=True, key="vol_scatter")
    st.caption(
        "**Cách đọc**: Góc phải trên = Volume cao + Giá tăng (breakout). "
        "Góc trái trên = Volume cao nhưng Giá không tăng (possible distribution). "
        "Đường ngang vàng = threshold 2× volume bình thường."
    )
else:
    charts._empty_fig
    st.info(
        "📊 **Volume Spike Scatter** cần `avg_volume_7d` từ baseline ≥7 ngày.\n\n"
        "Hiện tại pipeline chưa đủ lịch sử. Chart này sẽ tự động xuất hiện khi có data."
    )

st.markdown("---")

# ─── Section 4: Full Market Rank Table ────────────────────────────────────
st.subheader("🏆 Market Rank Snapshot")

df_rank = df_raw[
    [
        "market_cap_rank",
        "symbol",
        "display_name",
        "current_price",
        "price_change_percentage_24h",
        "total_volume",
        "rank_change",
        "fetched_at",
    ]
].copy()
df_rank = df_rank.sort_values("market_cap_rank")
df_rank["fetched_at"] = df_rank["fetched_at"].dt.strftime("%H:%M")
df_rank.columns = [
    "Rank",
    "Symbol",
    "Coin",
    "Price (USD)",
    "Change 24h %",
    "Volume 24h",
    "Rank Δ",
    "Time",
]

styled_rank = (
    df_rank.style.map(style_change, subset=["Change 24h %"])
    .map(style_rank, subset=["Rank Δ"])
    .format(
        {
            "Price (USD)": lambda x: (
                f"${x:,.6f}" if x < 0.001 else (f"${x:,.4f}" if x < 1 else f"${x:,.2f}")
            ),
            "Change 24h %": lambda x: f"{x:+.2f}%" if pd.notna(x) else "N/A",
            "Volume 24h": lambda x: f"${x / 1e9:.2f}B" if x >= 1e9 else f"${x / 1e6:.1f}M",
            "Rank Δ": lambda x: (
                f"{int(x):+d}" if pd.notna(x) and x != 0 else ("—" if x == 0 else "N/A")
            ),
        }
    )
)
st.dataframe(styled_rank, use_container_width=True, hide_index=True)
