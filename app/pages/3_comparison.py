"""
Page 4: Multi-Coin Comparison
Analytical story: "So sánh relative performance giữa các coins"

Sections:
  1. Coin multi-selector (mặc định loại stablecoins)
  2. Normalized price chart (Base = 100)
  3. Rolling Return grouped bar (7/30/90d)
  4. Risk-Return Scatter (Insight #3)
  5. Drawdown Heatmap (Insight #4)
  6. Correlation Matrix
"""

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import streamlit as st
import pandas as pd

from app.queries import (
    get_coin_list,
    get_normalized_prices,
    get_coin_performance_latest,
    get_daily_returns_wide,
)
from app import charts, theme



st.markdown("""
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
""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("## 📊 Crypto Dashboard")
    st.markdown("---")
    st.caption("Data: CoinGecko via pipeline")

# ─── Header ───────────────────────────────────────────────────────────────
st.markdown("# ⚖️ Multi-Coin Comparison")
st.markdown("*So sánh relative performance, risk, và correlation giữa các coins*")
st.markdown("---")

# ─── Coin Multi-Selector ──────────────────────────────────────────────────
coin_list_df = get_coin_list()
if coin_list_df.empty:
    st.error("⚠️ Không có dữ liệu dim_coin.")
    st.stop()

show_stable = st.sidebar.checkbox(
    "💧 Bao gồm Stablecoins",
    value=False,
    key="comp_stable",
    help=(
        "Stablecoins (Tether/USDT, USD Coin/USDC) được thiết kế để giữ giá ~$1 USD, "
        "không phản ánh biến động thị trường. Thêm vào chart so sánh thường tạo ra "
        "một đường thẳng phẳng gần 100 — hữu ích khi muốn dùng làm reference baseline."
    ),
)
available_coins = coin_list_df["coin_id"].tolist()
if not show_stable:
    available_coins = [c for c in available_coins if c not in theme.STABLECOINS]

# Default: BTC, ETH, SOL, BNB, XRP
default_selection = [c for c in ["bitcoin", "ethereum", "solana", "binancecoin", "ripple"]
                     if c in available_coins][:5]

selected_coins = st.multiselect(
    "Chọn Coins để so sánh",
    options=available_coins,
    default=default_selection,
    format_func=lambda x: theme.COIN_NAMES.get(x, x),
    max_selections=8,
    help="Chọn tối đa 8 coins để giữ chart rõ ràng",
    key="multi_coin_selector",
)

if not selected_coins:
    st.info("👆 Chọn ít nhất 2 coins để xem comparison charts.")
    st.stop()

if len(selected_coins) < 2:
    st.info("👆 Chọn thêm ít nhất 1 coin nữa để so sánh.")
    st.stop()

# ─── Section 1: Normalized Price ─────────────────────────────────────────
st.subheader("📈 Normalized Price (Base = 100)")
st.caption(
    "Giá được normalize về 100 tại ngày đầu tiên → dễ so sánh ai outperform ai, "
    "bất kể giá tuyệt đối khác nhau (BTC $64K vs. ADA $0.17)"
)

df_norm = get_normalized_prices(selected_coins)

if df_norm.empty or df_norm["normalized_price"].isna().all():
    st.info("Chưa có đủ data để normalize. Cần ≥1 ngày.")
else:
    fig_norm = charts.normalized_price_lines(df_norm, selected_coins)
    st.plotly_chart(fig_norm, use_container_width=True, key="norm_price")
    
    # Show quick winner/loser if ≥2 days
    latest_norm = df_norm.groupby("coin_id")["normalized_price"].last()
    if not latest_norm.empty and latest_norm.notna().any():
        winner = latest_norm.idxmax()
        loser  = latest_norm.idxmin()
        w_val  = latest_norm[winner]
        l_val  = latest_norm[loser]
        
        col_w, col_l = st.columns(2)
        col_w.success(f"🏆 **Best performer**: {theme.COIN_NAMES.get(winner, winner)} ({w_val:.1f})")
        col_l.error(f"⬇️ **Worst performer**: {theme.COIN_NAMES.get(loser, loser)} ({l_val:.1f})")

st.markdown("---")

# ─── Section 2: Rolling Return Grouped Bar ────────────────────────────────
st.subheader("📊 Rolling Returns (7 / 30 / 90 Days)")

df_perf_latest = get_coin_performance_latest()
df_perf_sel = df_perf_latest[df_perf_latest["coin_id"].isin(selected_coins)]

if df_perf_sel.empty:
    st.info("Performance data chưa có.")
else:
    fig_bar = charts.rolling_return_grouped_bar(df_perf_sel, selected_coins)
    st.plotly_chart(fig_bar, use_container_width=True, key="rolling_bar")
    
    # Show performance table
    tbl = df_perf_sel[["coin_id", "rolling_return_7d", "rolling_return_30d", "rolling_return_90d"]].copy()
    tbl["coin_id"] = tbl["coin_id"].map(theme.COIN_SYMBOLS).fillna(tbl["coin_id"])
    tbl.columns = ["Symbol", "Return 7d", "Return 30d", "Return 90d"]
    
    def fmt_pct(v):
        return f"{v*100:+.2f}%" if pd.notna(v) else "N/A"
    def style_ret(val):
        if isinstance(val, str): return "color: #8B949E"
        return ""
    
    st.dataframe(
        tbl.style.format({"Return 7d": fmt_pct, "Return 30d": fmt_pct, "Return 90d": fmt_pct}),
        use_container_width=True, hide_index=True
    )

st.markdown("---")

# ─── Section 3: Risk-Return Scatter (Insight #3) ─────────────────────────
st.subheader("🎯 Risk-Return Map — Insight #3")

with st.expander("ℹ️ Đọc Risk-Return Map"):
    st.markdown("""
**Trục X** = Volatility 30d (daily std) — đo lường **rủi ro**
**Trục Y** = Rolling Return 30d — đo lường **lợi nhuận**

**4 quadrant**:
| | Return thấp | Return cao |
|-|------------|-----------|
| **Volatility thấp** | 😐 Stagnant | 🌟 Ideal |
| **Volatility cao** | 💀 Worst | ⚡ High-risk |

> 💡 Không có "đúng/sai" — tùy risk tolerance của mỗi người. Stablecoin luôn ở góc dưới-trái.
""")

if df_perf_sel.empty or df_perf_sel[["volatility_30d", "rolling_return_30d"]].isna().all().all():
    st.info(
        "📊 Risk-Return Scatter cần ≥30 ngày data (volatility_30d + rolling_return_30d). "
        "Hiện tại pipeline chưa đủ lịch sử."
    )
else:
    fig_rr = charts.risk_return_scatter(df_perf_sel)
    st.plotly_chart(fig_rr, use_container_width=True, key="risk_return")

st.markdown("---")

# ─── Section 4: Drawdown Heatmap (Insight #4) ────────────────────────────
st.subheader("🔥 Drawdown Heatmap — Insight #4")

st.warning(
    "⚠️ **Disclaimer**: Drawdown tính từ đỉnh kể từ ngày pipeline bắt đầu chạy, "
    "không phải ATH lịch sử của từng coin."
)

# Load drawdown history for selected coins
from app.queries import get_conn
@st.cache_data(ttl=3600)
def get_drawdown_history(coin_ids: tuple) -> pd.DataFrame:
    sql = """
        SELECT coin_id, snapshot_date, drawdown_pct
        FROM marts.coin_performance_mart
        WHERE coin_id IN ({})
        ORDER BY snapshot_date, coin_id
    """.format(", ".join(f"'{c}'" for c in coin_ids))
    return get_conn().execute(sql).fetchdf()

df_dd = get_drawdown_history(tuple(selected_coins))

if df_dd.empty or df_dd["drawdown_pct"].isna().all():
    st.info("Drawdown data chưa có.")
else:
    fig_heatmap = charts.drawdown_heatmap(df_dd)
    st.plotly_chart(fig_heatmap, use_container_width=True, key="dd_heatmap")
    st.caption(
        "Color scale: 🟢 Xanh = gần đỉnh (drawdown nhỏ) | 🔴 Đỏ = xa đỉnh (drawdown lớn). "
        "Row = coin, Column = ngày."
    )

st.markdown("---")

# ─── Section 5: Correlation Matrix ───────────────────────────────────────
st.subheader("🔗 Return Correlation Matrix")

with st.expander("ℹ️ Đọc Correlation Matrix"):
    st.markdown("""
Ma trận correlation giữa **daily return** của các coins được chọn.

- **+1.0** (đỏ đậm) = hoàn toàn đồng pha — hai coins tăng giảm cùng nhau
- **0.0** (trung tính) = không có quan hệ tuyến tính
- **-1.0** (xanh đậm) = ngược pha hoàn toàn

⚠️ **Lưu ý**: Với chỉ 2 ngày data, correlation không có ý nghĩa thống kê. 
Cần ít nhất 30 ngày để có correlation đáng tin cậy.
""")

df_corr_wide = get_daily_returns_wide()
if not df_corr_wide.empty:
    # Filter to selected coins
    available_cols = [c for c in selected_coins if c in df_corr_wide.columns]
    if len(available_cols) >= 2:
        df_corr_filtered = df_corr_wide[available_cols]
        if df_corr_filtered.shape[0] < 2:
            st.info("Correlation matrix cần ≥2 ngày data (hiện tại: 1 ngày).")
        else:
            fig_corr = charts.correlation_heatmap(df_corr_filtered)
            st.plotly_chart(fig_corr, use_container_width=True, key="corr_matrix")
    else:
        st.info("Không đủ coins có data để tính correlation.")
else:
    st.info("Daily return data chưa có.")
