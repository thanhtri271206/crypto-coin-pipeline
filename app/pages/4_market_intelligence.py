"""
Page 5: Market Intelligence & Observability
Analytical story: "Pattern vĩ mô nào đang xuất hiện và hệ thống data pipeline đang vận hành ra sao?"

Tabs:
  1. 📈 Market Signals & Macro Intelligence (BTC Dominance, Concentration, Liquidity Velocity, Market Breadth)
  2. ⚙️ Data Pipeline & Warehouse Observability (SLA tracking, Live inventory, End-to-end lineage)
"""

import sys
from datetime import UTC, datetime
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import importlib

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from app import charts, queries, theme

# Reload queries if cached by long-running Streamlit process before edits
if not hasattr(queries, "get_warehouse_inventory"):
    importlib.reload(queries)

get_market_health_history = queries.get_market_health_history
get_market_overview = queries.get_market_overview
get_top_movers = queries.get_top_movers
get_warehouse_inventory = queries.get_warehouse_inventory

theme.apply_custom_css()

with st.sidebar:
    st.markdown("## 📊 Crypto Dashboard")
    st.markdown("---")
    st.caption("Data: CoinGecko via pipeline")

# ─── Header ───────────────────────────────────────────────────────────────
st.markdown("# 🧠 Market Intelligence & Observability")
st.markdown(
    "*Macro signals, market concentration, liquidity velocity, và pipeline health telemetry*"
)
st.markdown("---")

overview = get_market_overview()
history = get_market_health_history()

if overview is None or overview.empty:
    st.warning(
        "⚠️ Chưa có dữ liệu hoặc pipeline đang cập nhật market_health_mart. Vui lòng bấm Rerun sau giây lát."
    )
    st.stop()

row = overview.iloc[0]

# ─── Navigation Tabs ──────────────────────────────────────────────────────
tab_signals, tab_pipeline = st.tabs(
    [
        "📈 Market Signals & Macro Intelligence",
        "⚙️ Data Pipeline & Warehouse Observability",
    ]
)

# ══════════════════════════════════════════════════════════════════════════
# TAB 1: Market Signals & Macro Intelligence
# ══════════════════════════════════════════════════════════════════════════
with tab_signals:
    # ─── Section 1: BTC Dominance Regime (Insight #2) ─────────────────────
    st.subheader("🟠 BTC Dominance & Market Regime — Insight #2")

    with st.expander("ℹ️ BTC Dominance Regime là gì?"):
        st.markdown("""
**BTC Dominance** đo tỷ lệ % market cap của Bitcoin trong tổng thị trường crypto toàn cầu.

| BTC Dominance | Regime | Ý nghĩa thị trường |
|---|---|---|
| **> 55%** | 🟠 Bitcoin Season | Nhà đầu tư ưu tiên sự an toàn của BTC, altcoins underperform |
| **45–55%** | ⚪ Neutral Market | Cân bằng — không có xu hướng dòng tiền rõ ràng |
| **< 45%** | 🟣 Altcoin Season | Dòng tiền luân chuyển từ BTC sang altcoins tìm kiếm lợi nhuận |

> ⚠️ Đây là quan sát dữ liệu thị trường khách quan (Market Observation), không phải lời khuyên đầu tư.
""")

    btc_dom_now = row["btc_dominance_pct"]
    eth_dom_now = row["eth_dominance_pct"]

    # Regime logic
    if btc_dom_now >= 55:
        regime = "🟠 Bitcoin Season"
        regime_color = theme.COIN_COLORS.get("bitcoin", "#F7931A")
        regime_detail = (
            f"BTC chiếm **{btc_dom_now:.1f}%** vốn hóa toàn thị trường. "
            "Trong giai đoạn này, dòng vốn tập trung mạnh vào Bitcoin ('flight to quality/safety'). "
            "Hầu hết altcoins thường underperform so với BTC."
        )
    elif btc_dom_now <= 45:
        regime = "🟣 Altcoin Season"
        regime_color = theme.COIN_COLORS.get("ethereum", "#627EEA")
        regime_detail = (
            f"BTC dominance ở mức thấp ({btc_dom_now:.1f}%). "
            "Dòng tiền đang tích cực chảy sang các altcoins có beta cao hơn. "
            "Thị trường đang trong pha mở rộng khẩu vị rủi ro (Risk-On)."
        )
    else:
        regime = "⚪ Neutral Market"
        regime_color = theme.NEUTRAL_GRAY
        regime_detail = (
            f"BTC dominance ở mức trung tính ({btc_dom_now:.1f}%). "
            "Cung cầu cân bằng — thị trường đang trong giai đoạn tích lũy hoặc đi ngang (sideways)."
        )

    with st.container(border=True):
        st.markdown(
            f"<h3 style='color:{regime_color}; margin: 0 0 8px 0'>{regime}</h3>",
            unsafe_allow_html=True,
        )
        st.markdown(regime_detail)

    c1, c2, c3 = st.columns(3)
    c1.metric("BTC Dominance", f"{btc_dom_now:.2f}%")
    c2.metric("ETH Dominance", f"{eth_dom_now:.2f}%")
    c3.metric(
        "Others (Altcoins)",
        f"{100 - btc_dom_now - eth_dom_now:.2f}%",
        help="100% - BTC - ETH. Tỷ trọng vốn hóa của tất cả altcoins còn lại trên thị trường.",
    )

    if history is not None and history.shape[0] >= 2:
        fig_dom = charts.dominance_area(history)
        st.plotly_chart(fig_dom, width="stretch", key="intel_dominance")
    else:
        st.info("📊 Dominance trend chart sẽ hiển thị khi pipeline ghi nhận ≥2 snapshots.")

    st.markdown("---")

    # ─── Section 2: Market Concentration ─────────────────────────────────
    st.subheader("🏦 Market Concentration — Top 10 Coins")

    with st.expander("ℹ️ Market Concentration đo lường điều gì?"):
        st.markdown("""
**Top 10 Market Share** = Tổng vốn hóa của 10 đồng coin pipeline đang theo dõi / Tổng vốn hóa toàn thị trường.

- **Cao (>85%)**: Thị trường tập trung cao độ — các coin lớn chi phối hoàn toàn; thanh khoản tập trung tại nhóm dẫn dắt.
- **Thấp (<70%)**: Thị trường phân tán — dòng tiền đang đầu cơ vào hàng ngàn altcoins nhỏ bên ngoài.

**10 Coins được pipeline track**: Bitcoin, Ethereum, Tether, USD Coin, BNB, Solana, XRP, Dogecoin, Cardano, Shiba Inu.
""")

    top10_share = row.get("top10_market_share_pct")
    top10_mc = row.get("top10_market_cap_usd")
    total_mc = row.get("total_market_cap_usd")

    c1c, c2c = st.columns(2)
    c1c.metric(
        "Top 10 Market Share",
        f"{top10_share * 100:.2f}%" if pd.notna(top10_share) else "N/A",
        help="Tỷ lệ % market cap của 10 đồng coin lớn nhất so với toàn cầu",
    )
    c2c.metric(
        "Top 10 Market Cap",
        f"${top10_mc / 1e12:.3f}T"
        if pd.notna(top10_mc) and top10_mc >= 1e12
        else (f"${top10_mc / 1e9:.1f}B" if pd.notna(top10_mc) else "N/A"),
    )

    if (
        history is not None
        and history.shape[0] >= 2
        and "top10_market_share_pct" in history.columns
    ):
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
            annotation_text="80% Baseline",
            annotation_font_size=10,
        )
        fig_conc.update_layout(
            template=theme.CHART_TEMPLATE,
            paper_bgcolor=theme.CHART_PAPER_BG,
            plot_bgcolor=theme.CHART_PLOT_BG,
            font=dict(color=theme.CHART_FONT_COLOR, family="Inter, sans-serif", size=12),
            title=dict(
                text="Top 10 Market Concentration (%) Over Time",
                font=dict(size=14, color=theme.TEXT_PRIMARY),
            ),
            margin=theme.CHART_MARGIN,
            height=300,
            xaxis=dict(gridcolor=theme.BORDER, showgrid=True),
            yaxis=dict(gridcolor=theme.BORDER, showgrid=True, title="Share %"),
        )
        st.plotly_chart(fig_conc, width="stretch", key="concentration")
    else:
        st.info("📊 Concentration trend sẽ xuất hiện khi có ≥2 snapshots.")

    st.markdown("---")

    # ─── Section 3: Liquidity Velocity & Market Breadth (MỚI) ─────────────
    st.subheader("💧 Liquidity Velocity & Market Breadth")

    with st.expander("ℹ️ Ý nghĩa chỉ báo Liquidity Velocity & Market Breadth"):
        st.markdown("""
- **Liquidity Velocity (Volume / Market Cap)**: Đo lường tốc độ quay vòng vốn của thị trường crypto.
  - **> 5%**: Dòng tiền giao dịch rất nóng, thị trường biến động mạnh.
  - **2% – 5%**: Mức độ thanh khoản bình thường.
  - **< 2%**: Thị trường cạn kiệt thanh khoản (Dry Market), nhà đầu tư đứng ngoài quan sát.
- **Market Breadth (Độ rộng thị trường 24h)**: Tỷ lệ % các đồng coin có biến động giá dương.
  - **> 60%**: Đà tăng lan tỏa diện rộng (Broad Market Rally).
  - **40% – 60%**: Thị trường phân hóa rõ rệt.
  - **< 40%**: Sắc đỏ chiếm đa số, đà giảm lan rộng.
""")

    total_vol = row.get("total_volume_usd", 0)
    turnover_pct = (total_vol / total_mc * 100) if total_mc and total_mc > 0 else 0

    if turnover_pct >= 5.0:
        turnover_label = "🔥 Cao (Active / Volatile)"
        turnover_desc = (
            "Tốc độ luân chuyển tiền mặt rất nhanh, thị trường đang đón nhận dòng tiền lớn."
        )
    elif turnover_pct >= 2.0:
        turnover_label = "⚖️ Trung bình (Normal)"
        turnover_desc = "Thanh khoản ở mức tiêu chuẩn, giao dịch ổn định."
    else:
        turnover_label = "❄️ Thấp (Low Liquidity)"
        turnover_desc = "Khối lượng giao dịch thấp so với quy mô vốn hóa, thị trường ảm đạm."

    # Tính Market Breadth từ top movers
    df_movers = get_top_movers()
    if df_movers is not None and not df_movers.empty:
        df_no_st = df_movers[~df_movers["coin_id"].isin(theme.STABLECOINS)]
        total_coins = len(df_no_st)
        gainers_count = int((df_no_st["price_change_percentage_24h"] > 0).sum())
        breadth_pct = (gainers_count / total_coins * 100) if total_coins > 0 else 0

        if breadth_pct >= 60:
            breadth_state = f"🟢 Lan tỏa tích cực ({gainers_count}/{total_coins} coins xanh)"
        elif breadth_pct >= 40:
            breadth_state = f"⚪ Thị trường phân hóa ({gainers_count}/{total_coins} coins xanh)"
        else:
            breadth_state = f"🔴 Áp lực bán diện rộng ({gainers_count}/{total_coins} coins xanh)"
    else:
        breadth_pct = 0
        breadth_state = "Chưa có dữ liệu"

    col_l1, col_l2 = st.columns(2)
    col_l1.metric(
        "Liquidity Velocity (Vol / MCap)",
        f"{turnover_pct:.2f}%",
        delta=turnover_label,
        help="Tỷ lệ Tổng Volume 24h / Tổng Vốn Hóa. Chỉ số càng cao thể hiện dòng tiền sôi động.",
    )
    col_l2.metric(
        "Market Breadth (24h Gainers)",
        f"{breadth_pct:.0f}%",
        delta=breadth_state,
        help="Tỷ lệ phần trăm các coin phi-stablecoin có biến động giá 24h dương.",
    )

    st.caption(f"💡 **Tình trạng thanh khoản**: {turnover_desc}")


# ══════════════════════════════════════════════════════════════════════════
# TAB 2: Data Pipeline & Warehouse Observability
# ══════════════════════════════════════════════════════════════════════════
with tab_pipeline:
    st.subheader("⏱️ Pipeline SLA & Data Freshness Tracking")

    fetched_at = row["fetched_at"]
    now_utc = datetime.now(UTC).replace(tzinfo=None)
    staleness_min = (now_utc - fetched_at.to_pydatetime().replace(tzinfo=None)).total_seconds() / 60

    # Đánh giá SLA theo tiêu chuẩn Data Engineering
    # Hourly pipeline có SLA là 60 phút (cho phép buffer 15 phút là 75 phút)
    if staleness_min <= 75:
        sla_status = "🟢 IN SLA (<60m)"
        sla_color = theme.POSITIVE_GREEN
    elif staleness_min <= 180:
        sla_status = "🟡 SLA WARNING"
        sla_color = theme.WARN_AMBER
    else:
        sla_status = "🔴 SLA BREACHED (>3h)"
        sla_color = theme.NEGATIVE_RED

    c1d, c2d, c3d = st.columns(3)
    c1d.metric("Last Ingestion (UTC)", fetched_at.strftime("%Y-%m-%d %H:%M"))
    c2d.metric(
        "Pipeline Freshness",
        f"{staleness_min:.0f} min ago",
        delta=sla_status,
        help="Khoảng cách thời gian từ snapshot gần nhất tới hiện tại. Target SLA: <60 phút.",
    )
    c3d.metric(
        "Historical Snapshots",
        f"{len(history)} runs",
        help="Tổng số snapshots toàn cầu đã tích lũy trong DuckDB warehouse.",
    )

    st.markdown("---")

    # ─── Live Data Warehouse Inventory ────────────────────────────────────
    st.subheader("📦 Live Data Warehouse Inventory (Medallion Architecture)")
    st.markdown(
        "*Kiểm kê động số lượng bản ghi thực tế theo kiến trúc phân tầng Medallion trong DuckDB Warehouse:*"
    )

    inv_df = get_warehouse_inventory()
    if inv_df is not None and not inv_df.empty:
        total_rows = inv_df["row_count"].sum()
        total_tbls = len(inv_df)

        c_inv1, c_inv2 = st.columns(2)
        c_inv1.metric("Total Warehouse Tables", f"{total_tbls} tables")
        c_inv2.metric("Total Stored Records", f"{total_rows:,} rows")

        styled_inv = inv_df.style.format({"row_count": "{:,}"}).set_properties(
            **{"text-align": "left"}
        )
        st.dataframe(styled_inv, width="stretch", hide_index=True)
    else:
        st.info("Chưa thể tải warehouse inventory. Pipeline đang cập nhật bảng.")

    st.markdown("---")

    # ─── End-to-End Data Pipeline Architecture ───────────────────────────
    st.subheader("🏗️ End-to-End Data Pipeline Lineage")

    st.markdown("""
Dữ liệu di chuyển tuần tự qua 4 phân tầng công nghệ theo chuẩn **ELT Hiện Đại**:
""")

    st.markdown(
        """
```mermaid
flowchart LR
    subgraph S1["1. Ingestion Layer (Raw)"]
        API["🌐 CoinGecko API"] -->|hourly HTTP| AIRFLOW["⏱️ Apache Airflow"]
        AIRFLOW -->|idempotent JSON| S3["🪣 MinIO / S3 Lake (Bronze)"]
    end

    subgraph S2["2. Transformation Layer (dbt)"]
        S3 -->|read JSON| STG["📑 dbt Staging (Silver)"]
        STG -->|dbt build + tests| CORE["🏛️ dbt Core Models<br/>dim_coin, fct_hourly, fct_daily"]
        CORE -->|rolling window SQL| MARTS["🏆 dbt Marts (Gold)<br/>market_health, top_movers, coin_perf"]
    end

    subgraph S3["3. Analytics & Serving"]
        MARTS -->|DuckDB read-only| APP["📊 Streamlit Multi-Page App"]
    end
```
""",
        unsafe_allow_html=True,
    )

    with st.expander("🔍 Chi tiết Data Quality & SLA Design"):
        st.markdown("""
1. **Idempotency**: Dữ liệu tải lên MinIO/S3 được phân chia theo `fetched_at` và `logical_date` của Airflow, cho phép backfill an toàn không trùng lặp.
2. **Schema Validation**: Trước khi chuyển sang warehouse, dữ liệu thô được validate tự động qua **Pydantic Schemas** (`CoinGeckoMarketsRaw`, `GlobalMarketDataRaw`).
3. **dbt Integrity Tests**: Mỗi lần chạy `dbt build`, hệ thống kiểm tra tính toàn vẹn:
   - `unique` & `not_null` trên các primary keys.
   - `relationships` giữa fact tables và dimension tables (`coin_id` $\\leftrightarrow$ `dim_coin`).
   - `dbt source freshness` để cảnh báo nếu dữ liệu đầu nguồn bị trễ SLA.
""")

st.markdown("---")
st.markdown(
    "<p style='color: #6E7681; font-size: 12px; text-align: center'>"
    "Data source: CoinGecko API · Orchestrator: Airflow · Storage: MinIO S3 & DuckDB · Transform: dbt · Serving: Streamlit"
    "</p>",
    unsafe_allow_html=True,
)
