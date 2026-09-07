"""
Data access layer — tất cả SQL queries tập trung tại đây.
Mỗi function trả về pd.DataFrame đã sẵn sàng cho visualization.

Cache policy:
  - REALTIME (5 min): market overview, top movers — thay đổi mỗi khi pipeline chạy
  - DAILY (1 hr): performance mart — grain là ngày, ít thay đổi
  - STATIC (24 hr): dim_coin — metadata ít thay đổi

Schema note (từ dbt_project.yml):
  - core.*   : dim_coin, dim_time, fct_market_snapshot_hourly/daily, fct_global_market_snapshot
  - marts.*  : market_health_mart, top_movers_mart, coin_performance_mart
"""

import pandas as pd
import streamlit as st

from app.db import get_conn

TTL_REALTIME = 300  # 5 phút
TTL_DAILY = 3600  # 1 giờ
TTL_STATIC = 86400  # 24 giờ


# ════════════════════════════════════════════════════════════════════════════
# GROUP 1: Market Health (macro view)
# ════════════════════════════════════════════════════════════════════════════


@st.cache_data(ttl=TTL_REALTIME)
def get_market_overview() -> pd.DataFrame:
    """
    Snapshot mới nhất từ market_health_mart.
    Return: 1 row với tất cả KPIs.
    """
    sql = """
        SELECT
            fetched_at,
            snapshot_date_id,
            total_market_cap_usd,
            total_volume_usd,
            btc_dominance_pct,
            eth_dominance_pct,
            top10_market_cap_usd,
            top10_market_share_pct,
            market_cap_change_pct_24h,
            volume_change_pct_24h
        FROM marts.market_health_mart
        ORDER BY fetched_at DESC
        LIMIT 1
    """
    return get_conn().execute(sql).fetchdf()


@st.cache_data(ttl=TTL_DAILY)
def get_market_health_history() -> pd.DataFrame:
    """
    Toàn bộ lịch sử market_health_mart, sắp theo thời gian.
    Dùng cho trend charts (market cap, dominance over time).
    """
    sql = """
        SELECT
            fetched_at,
            total_market_cap_usd,
            total_volume_usd,
            btc_dominance_pct,
            eth_dominance_pct,
            top10_market_share_pct,
            market_cap_change_pct_24h
        FROM marts.market_health_mart
        ORDER BY fetched_at ASC
    """
    return get_conn().execute(sql).fetchdf()


# ════════════════════════════════════════════════════════════════════════════
# GROUP 2: Top Movers (real-time snapshot)
# ════════════════════════════════════════════════════════════════════════════


@st.cache_data(ttl=TTL_REALTIME)
def get_top_movers() -> pd.DataFrame:
    """
    Snapshot mới nhất của từng coin từ top_movers_mart.
    Grain: 1 row / coin (latest fetched_at).
    Bao gồm stablecoins — caller tự filter nếu cần.
    """
    sql = """
        SELECT
            coin_id,
            fetched_at,
            snapshot_date,
            current_price,
            price_change_percentage_24h,
            price_change_percentage_7d_in_currency,
            market_cap_rank,
            rank_change,
            total_volume,
            avg_volume_7d,
            volume_spike_ratio
        FROM marts.top_movers_mart
        ORDER BY market_cap_rank ASC
    """
    return get_conn().execute(sql).fetchdf()


# ════════════════════════════════════════════════════════════════════════════
# GROUP 3: Coin Performance (daily rolling metrics)
# ════════════════════════════════════════════════════════════════════════════


@st.cache_data(ttl=TTL_DAILY)
def get_coin_performance_latest() -> pd.DataFrame:
    """
    Row mới nhất của từng coin trong coin_performance_mart.
    Dùng cho bảng so sánh performance.
    """
    sql = """
        SELECT
            coin_id,
            snapshot_date,
            rolling_return_7d,
            rolling_return_30d,
            rolling_return_90d,
            volatility_7d,
            volatility_30d,
            drawdown_pct,
            max_drawdown_30d
        FROM marts.coin_performance_mart
        QUALIFY ROW_NUMBER() OVER (PARTITION BY coin_id ORDER BY snapshot_date DESC) = 1
        ORDER BY coin_id
    """
    return get_conn().execute(sql).fetchdf()


@st.cache_data(ttl=TTL_DAILY)
def get_coin_performance_history(coin_id: str) -> pd.DataFrame:
    """
    Lịch sử performance của một coin theo ngày.
    Dùng cho deep dive chart: rolling return, volatility, drawdown.
    """
    sql = """
        SELECT
            snapshot_date,
            rolling_return_7d,
            rolling_return_30d,
            rolling_return_90d,
            volatility_7d,
            volatility_30d,
            drawdown_pct,
            max_drawdown_30d
        FROM marts.coin_performance_mart
        WHERE coin_id = ?
        ORDER BY snapshot_date ASC
    """
    return get_conn().execute(sql, [coin_id]).fetchdf()


# ════════════════════════════════════════════════════════════════════════════
# GROUP 4: Price History (fact tables)
# ════════════════════════════════════════════════════════════════════════════


@st.cache_data(ttl=TTL_DAILY)
def get_daily_prices(coin_id: str | None = None) -> pd.DataFrame:
    """
    OHLCV theo ngày từ fct_market_snapshot_daily.
    Nếu coin_id=None, trả về toàn bộ (all coins, all dates).
    """
    if coin_id:
        sql = """
            SELECT
                coin_id,
                snapshot_date,
                open,
                high,
                low,
                close,
                volume_24h_rolling,
                daily_return
            FROM core.fct_market_snapshot_daily
            WHERE coin_id = ?
            ORDER BY snapshot_date ASC
        """
        return get_conn().execute(sql, [coin_id]).fetchdf()
    else:
        sql = """
            SELECT
                coin_id,
                snapshot_date,
                open,
                high,
                low,
                close,
                volume_24h_rolling,
                daily_return
            FROM core.fct_market_snapshot_daily
            ORDER BY snapshot_date ASC, coin_id
        """
        return get_conn().execute(sql).fetchdf()


@st.cache_data(ttl=TTL_REALTIME)
def get_hourly_prices(coin_id: str | None = None) -> pd.DataFrame:
    """
    Price snapshots theo hourly từ fct_market_snapshot_hourly.
    Nếu coin_id=None, trả về toàn bộ.
    """
    if coin_id:
        sql = """
            SELECT
                coin_id,
                fetched_at,
                current_price,
                market_cap,
                total_volume,
                high_24h,
                low_24h,
                price_change_percentage_24h,
                market_cap_rank
            FROM core.fct_market_snapshot_hourly
            WHERE coin_id = ?
            ORDER BY fetched_at ASC
        """
        return get_conn().execute(sql, [coin_id]).fetchdf()
    else:
        sql = """
            SELECT
                coin_id,
                fetched_at,
                current_price,
                market_cap,
                total_volume,
                price_change_percentage_24h,
                market_cap_rank
            FROM core.fct_market_snapshot_hourly
            ORDER BY fetched_at ASC, coin_id
        """
        return get_conn().execute(sql).fetchdf()


# ════════════════════════════════════════════════════════════════════════════
# GROUP 5: Dimensions
# ════════════════════════════════════════════════════════════════════════════


@st.cache_data(ttl=TTL_STATIC)
def get_coin_list() -> pd.DataFrame:
    """
    Danh sách coins từ dim_coin, sắp theo market_cap_rank_static.
    Dùng cho dropdown selectors.
    """
    sql = """
        SELECT
            coin_id,
            symbol,
            name,
            categories,
            genesis_date,
            market_cap_rank_static,
            description_en
        FROM core.dim_coin
        ORDER BY market_cap_rank_static ASC NULLS LAST
    """
    return get_conn().execute(sql).fetchdf()


@st.cache_data(ttl=TTL_STATIC)
def get_coin_metadata(coin_id: str) -> pd.DataFrame:
    """Metadata của một coin cụ thể từ dim_coin."""
    sql = """
        SELECT
            coin_id,
            symbol,
            name,
            categories,
            genesis_date,
            homepage_url,
            description_en,
            market_cap_rank_static,
            metadata_last_updated
        FROM core.dim_coin
        WHERE coin_id = ?
        LIMIT 1
    """
    return get_conn().execute(sql, [coin_id]).fetchdf()


# ════════════════════════════════════════════════════════════════════════════
# GROUP 6: Comparison (multi-coin analytics)
# ════════════════════════════════════════════════════════════════════════════


@st.cache_data(ttl=TTL_DAILY)
def get_normalized_prices(coin_ids: list[str] | None = None) -> pd.DataFrame:
    """
    Giá normalized về 100 từ ngày đầu tiên — so sánh relative performance.
    Nguồn: fct_market_snapshot_daily.close

    Coin_ids=None → trả về toàn bộ coins.
    """
    sql = """
        WITH base AS (
            SELECT
                coin_id,
                snapshot_date,
                close,
                FIRST_VALUE(close) OVER (
                    PARTITION BY coin_id ORDER BY snapshot_date ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS base_price
            FROM core.fct_market_snapshot_daily
            {where_clause}
        )
        SELECT
            coin_id,
            snapshot_date,
            close,
            base_price,
            CASE WHEN base_price > 0 
                 THEN (close / base_price) * 100 
                 ELSE NULL END AS normalized_price
        FROM base
        ORDER BY snapshot_date ASC, coin_id
    """
    if coin_ids:
        placeholders = ", ".join("?" * len(coin_ids))
        where = f"WHERE coin_id IN ({placeholders})"
        sql_final = sql.format(where_clause=where)
        return get_conn().execute(sql_final, coin_ids).fetchdf()
    else:
        sql_final = sql.format(where_clause="")
        return get_conn().execute(sql_final).fetchdf()


@st.cache_data(ttl=TTL_DAILY)
def get_daily_returns_wide() -> pd.DataFrame:
    """
    Daily returns của tất cả coins dạng wide (coin_id là columns).
    Dùng cho correlation matrix.
    """
    sql = """
        SELECT snapshot_date, coin_id, daily_return
        FROM core.fct_market_snapshot_daily
        ORDER BY snapshot_date ASC
    """
    df = get_conn().execute(sql).fetchdf()
    if df.empty:
        return df
    return df.pivot(index="snapshot_date", columns="coin_id", values="daily_return")
