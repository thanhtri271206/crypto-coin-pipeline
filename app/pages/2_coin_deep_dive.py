"""
Page 3: Coin Deep Dive
Analytical story: "Với một coin cụ thể, performance và risk như thế nào?"

Sections:
  1. Coin selector + metadata
  2. Price history (hourly snapshots)
  3. Daily Return bar chart
  4. Rolling Returns (7/30/90d) — graceful empty nếu chưa đủ ngày
  5. Drawdown area chart + mandatory disclaimer
  6. Volatility KPI cards
"""

import importlib
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pandas as pd
import streamlit as st

from app import charts, theme

# Reload theme if cached by a long-running Streamlit process before edits
if not hasattr(theme, "COIN_DESCRIPTIONS_VI"):
    importlib.reload(theme)
from app.queries import (
    get_coin_list,
    get_coin_metadata,
    get_coin_performance_history,
    get_daily_prices,
    get_hourly_prices,
)

theme.apply_custom_css()

with st.sidebar:
    st.markdown("## 📊 Crypto Dashboard")
    st.markdown("---")
    st.caption("Data: CoinGecko via pipeline")

# ─── Header ───────────────────────────────────────────────────────────────
st.markdown("# 🔍 Coin Deep Dive")
st.markdown("*Phân tích chi tiết performance và risk của một coin*")
st.markdown("---")

# ─── Coin Selector ────────────────────────────────────────────────────────
coin_list_df = get_coin_list()
if coin_list_df is None or coin_list_df.empty:
    st.warning(
        "⚠️ Chưa có dữ liệu dim_coin hoặc pipeline đang đồng bộ. Vui lòng bấm Rerun sau giây lát."
    )
    st.stop()

# Build options: exclude stablecoins mặc định, cho phép opt-in
show_stable = st.sidebar.checkbox(
    "💧 Hiển thị Stablecoins",
    value=False,
    help=(
        "Stablecoins (Tether/USDT, USD Coin/USDC) là các loại tiền mã được thiết kế "
        "để duy trì giá cố định ~$1 USD. Chúng không biến động theo thị trường, "
        "nên phần lận chỉ số kỹ thuật (return, volatility, drawdown) của chúng "
        "gần như = 0. Mặc định ẩn đi để tập trung vào các coin có biến động thực sự."
    ),
)
if not show_stable:
    coin_list_df = coin_list_df[~coin_list_df["coin_id"].isin(theme.STABLECOINS)]

options = coin_list_df["coin_id"].tolist()
option_labels = {cid: theme.COIN_NAMES.get(cid, cid) for cid in options}

selected_coin = st.selectbox(
    "Chọn Coin",
    options=options,
    format_func=lambda x: option_labels.get(x, x),
    index=0,
    key="coin_selector",
)

coin_color = theme.COIN_COLORS.get(selected_coin, theme.ACCENT_BLUE)
coin_label = theme.COIN_NAMES.get(selected_coin, selected_coin)

# ─── Coin Metadata Header ─────────────────────────────────────────────────
meta = get_coin_metadata(selected_coin)
if meta is not None and not meta.empty:
    m = meta.iloc[0]

    col_info, col_genesis = st.columns([4, 1])
    with col_info:
        st.markdown(
            f"<h2 style='color:{coin_color}; margin-bottom:4px'>● {coin_label}</h2>",
            unsafe_allow_html=True,
        )
    with col_genesis:
        genesis = m.get("genesis_date")
        if genesis and pd.notna(genesis):
            st.metric("Genesis Date", str(genesis)[:10])

# ─── Load price data ──────────────────────────────────────────────────────
df_hourly = get_hourly_prices(selected_coin)
df_daily = get_daily_prices(selected_coin)
df_perf = get_coin_performance_history(selected_coin)

# ─── KPI snapshot ─────────────────────────────────────────────────────────
if df_hourly is not None and not df_hourly.empty:
    latest_h = df_hourly.sort_values("fetched_at").iloc[-1]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric(
        "Current Price",
        f"${latest_h['current_price']:,.4f}"
        if latest_h["current_price"] < 1
        else f"${latest_h['current_price']:,.2f}",
    )
    pc_24h = latest_h.get("price_change_percentage_24h")
    c2.metric(
        "24h Change",
        f"{pc_24h:+.2f}%" if pd.notna(pc_24h) else "N/A",
    )
    c3.metric(
        "Market Cap Rank",
        f"#{int(latest_h['market_cap_rank'])}"
        if pd.notna(latest_h.get("market_cap_rank"))
        else "N/A",
    )
    mc = latest_h.get("market_cap")
    c4.metric(
        "Market Cap",
        f"${mc / 1e9:.1f}B"
        if pd.notna(mc) and mc >= 1e9
        else (f"${mc / 1e6:.0f}M" if pd.notna(mc) else "N/A"),
    )
    st.caption(f"🕐 Snapshot: {latest_h['fetched_at']} UTC")

st.markdown("---")

# ─── Section 1: Price Chart ───────────────────────────────────────────────
st.subheader("💰 Price History")

# Long-term daily chart — luôn ưu tiên hiện nếu có data (366 ngày backfill)
if df_daily is not None and df_daily.shape[0] >= 1:
    fig_price = charts.price_line(df_daily, selected_coin, use_hourly=False)
    st.plotly_chart(fig_price, width="stretch", key=f"price_daily_{selected_coin}")
    st.caption(f"Granularity: daily close ({df_daily.shape[0]} ngày)")
else:
    st.info(f"📊 Chưa có price data cho {coin_label}")

# Intraday hourly chart — chỉ hiện khi pipeline đã chạy đủ lâu để có nhiều ngày hourly
if df_hourly is not None and df_hourly.shape[0] >= 2:
    with st.expander(
        f"🔍 Intraday View — Hourly Snapshots ({df_hourly.shape[0]} points)", expanded=False
    ):
        fig_hourly = charts.price_line(df_hourly, selected_coin, use_hourly=True)
        st.plotly_chart(fig_hourly, width="stretch", key=f"price_hourly_{selected_coin}")
        st.caption(
            "Granularity: hourly snapshots từ pipeline. "
            "Khi pipeline chạy lâu hơn, chart này sẽ cover nhiều ngày hơn."
        )

# ─── Section 2: Daily Return ──────────────────────────────────────────────
st.markdown("---")
st.subheader("📊 Daily Return")

if df_daily is not None and not df_daily.empty:
    fig_ret = charts.daily_return_bar(df_daily, selected_coin)
    st.plotly_chart(fig_ret, width="stretch", key=f"return_{selected_coin}")
    st.caption(
        "Daily Return = (Close - Open) / Open trong ngày. "
        "🟢 Xanh = dương (close > open) | 🔴 Đỏ = âm (close < open)"
    )
else:
    st.info("Daily return data chưa có.")

# ─── Section 3: Rolling Returns ───────────────────────────────────────────
st.markdown("---")
st.subheader("📈 Rolling Returns (7 / 30 / 90 Days)")

with st.expander("ℹ️ Rolling Return là gì?"):
    st.markdown("""
**Rolling Return 7d** = (Giá hôm nay − Giá 7 ngày trước) / Giá 7 ngày trước

Đây là **tổng lợi nhuận gộp** trong khoảng thời gian đó, không phải lợi nhuận trung bình mỗi ngày.

| Thời gian | Sẵn có khi | Ý nghĩa |
|-----------|-----------|---------|
| 7-Day | ≥7 ngày data | Short-term momentum |
| 30-Day | ≥30 ngày data | Medium-term trend |
| 90-Day | ≥90 ngày data | Long-term performance |
""")

if df_perf is not None and not df_perf.empty:
    fig_rolling = charts.rolling_return_lines(df_perf, selected_coin)
    st.plotly_chart(fig_rolling, width="stretch", key=f"rolling_{selected_coin}")
else:
    st.info("Rolling return data chưa có. Cần ít nhất 7 ngày data sau khi pipeline chạy liên tục.")

# ─── Section 4: Volatility ────────────────────────────────────────────────
st.markdown("---")
st.subheader("⚡ Volatility")

if not df_perf.empty:
    latest_perf = df_perf.sort_values("snapshot_date").iloc[-1]
    vol7 = latest_perf.get("volatility_7d")
    vol30 = latest_perf.get("volatility_30d")

    cv1, cv2, cv3 = st.columns(3)
    cv1.metric(
        "Volatility 7d",
        f"{vol7 * 100:.3f}%" if pd.notna(vol7) else "N/A (chưa đủ 7 ngày)",
        help="Độ lệch chuẩn daily return trong 7 ngày. Càng cao = biến động càng mạnh.",
    )
    cv2.metric(
        "Volatility 30d",
        f"{vol30 * 100:.3f}%" if pd.notna(vol30) else "N/A (chưa đủ 30 ngày)",
        help="Độ lệch chuẩn daily return trong 30 ngày.",
    )
    with cv3:
        if pd.notna(vol7):
            # Annualized (crypto thường dùng 365 ngày, không phải 252)
            vol_ann = vol7 * (365**0.5)
            st.metric(
                "Volatility 7d (Annualized)",
                f"{vol_ann * 100:.1f}%",
                help="vol_7d × √365. Tiêu chuẩn dùng cho crypto (365 ngày giao dịch/năm).",
            )
        else:
            st.metric("Volatility 7d (Annualized)", "N/A")

    with st.expander("ℹ️ Đọc Volatility Crypto"):
        st.markdown("""
Volatility = **độ lệch chuẩn (std)** của daily return.

- **Crypto thường có volatility 3-10% mỗi ngày** (vs. chứng khoán ~0.5-1%/ngày)
- Bitcoin thường ~3-5%/ngày; altcoins nhỏ có thể 5-15%/ngày
- Volatility cao = rủi ro cao nhưng cũng có cơ hội lợi nhuận cao hơn
- **Không so sánh trực tiếp với volatility của cổ phiếu**
""")
else:
    st.info("Volatility data chưa có. Cần ≥2 ngày data để tính.")

# ─── Section 5: Drawdown ──────────────────────────────────────────────────
st.markdown("---")
st.subheader("📉 Drawdown from Peak")

# Mandatory disclaimer — luôn hiển thị
st.warning(
    "⚠️ **Disclaimer bắt buộc**: Drawdown ở đây được tính từ **đỉnh giá kể từ ngày pipeline bắt đầu chạy**, "
    "**KHÔNG PHẢI** All-Time High (ATH) lịch sử của coin. "
    r"Ví dụ: Bitcoin ATH lịch sử ~\$109K nhưng pipeline bắt đầu tại ~\$64K → drawdown hiện tại từ \$64K."
)

if df_perf is not None and not df_perf.empty:
    # Tìm ngày bắt đầu pipeline
    pipeline_start = str(df_perf["snapshot_date"].min())[:10] if not df_perf.empty else ""
    fig_dd = charts.drawdown_area(df_perf, selected_coin, pipeline_start)
    st.plotly_chart(fig_dd, width="stretch", key=f"drawdown_{selected_coin}")

    latest_dd = df_perf.sort_values("snapshot_date").iloc[-1]
    dd_pct = latest_dd.get("drawdown_pct")
    dd_30 = latest_dd.get("max_drawdown_30d")

    ddc1, ddc2 = st.columns(2)
    ddc1.metric(
        "Current Drawdown",
        f"{dd_pct * 100:.2f}%" if pd.notna(dd_pct) else "N/A",
        help="(Close - Pipeline Peak) / Pipeline Peak. = 0% khi coin ở đỉnh pipeline.",
    )
    ddc2.metric(
        "Worst Drawdown (30d)",
        f"{dd_30 * 100:.2f}%" if pd.notna(dd_30) else "N/A",
        help="Drawdown tệ nhất trong 30 ngày gần nhất. Đo mức tổn thất tối đa trong tháng.",
    )
else:
    st.info("Drawdown data chưa có.")

# ─── Coin Description ─────────────────────────────────────────────────────
coin_desc_dict = getattr(theme, "COIN_DESCRIPTIONS_VI", {})
desc_vi = coin_desc_dict.get(selected_coin)
if desc_vi:
    st.markdown("---")
    with st.expander(f"📄 Về {coin_label}", expanded=True):
        st.markdown(desc_vi)
elif meta is not None and not meta.empty and pd.notna(meta.iloc[0].get("description_en")):
    desc = meta.iloc[0]["description_en"]
    if desc and len(desc) > 10:
        st.markdown("---")
        with st.expander(f"📄 Về {coin_label}"):
            # Limit to first 500 chars (avoid very long descriptions)
            st.markdown(desc[:500] + ("..." if len(desc) > 500 else ""))
