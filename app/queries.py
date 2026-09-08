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


def _safe_query(sql: str, params: list | None = None) -> pd.DataFrame:
    """Execute SQL safely against DuckDB connection using a thread-local cursor; return empty DataFrame on error."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        if params is not None:
            return cur.execute(sql, params).fetchdf()
        return cur.execute(sql).fetchdf()
    except Exception as e:
        st.warning(f"⚠️ Chưa thể tải dữ liệu từ warehouse (pipeline có thể đang cập nhật): {e}")
        return pd.DataFrame()


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
    return _safe_query(sql)


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
    return _safe_query(sql)


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
    return _safe_query(sql)


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
    return _safe_query(sql)


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
    return _safe_query(sql, [coin_id])


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
        return _safe_query(sql, [coin_id])
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
        return _safe_query(sql)


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
        return _safe_query(sql, [coin_id])
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
        return _safe_query(sql)


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
    return _safe_query(sql)


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
    return _safe_query(sql, [coin_id])


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
        return _safe_query(sql_final, coin_ids)
    else:
        sql_final = sql.format(where_clause="")
        return _safe_query(sql_final)


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
    df = _safe_query(sql)
    if df.empty:
        return df
    return df.pivot(index="snapshot_date", columns="coin_id", values="daily_return")


# ════════════════════════════════════════════════════════════════════════════
# GROUP 7: Data Pipeline & Warehouse Observability
# ════════════════════════════════════════════════════════════════════════════


@st.cache_data(ttl=TTL_REALTIME)
def get_warehouse_inventory() -> pd.DataFrame:
    """
    Thống kê động số lượng records của các bảng trong Data Warehouse
    phân theo kiến trúc Medallion (Core / Silver và Marts / Gold).
    """
    sql = """
        SELECT
            'core.dim_coin' AS table_name,
            'Core (Silver)' AS layer,
            'Metadata 10 đồng coins' AS description,
            '1 row / coin' AS grain,
            count(*) AS row_count
        FROM core.dim_coin
        UNION ALL
        SELECT
            'core.dim_time',
            'Core (Silver)',
            'Date dimension calendar',
            '1 row / ngày',
            count(*)
        FROM core.dim_time
        UNION ALL
        SELECT
            'core.fct_market_snapshot_hourly',
            'Core (Silver)',
            'Hourly price & market snapshots',
            '1 row / coin / giờ',
            count(*)
        FROM core.fct_market_snapshot_hourly
        UNION ALL
        SELECT
            'core.fct_market_snapshot_daily',
            'Core (Silver)',
            'Daily OHLCV & volume history',
            '1 row / coin / ngày',
            count(*)
        FROM core.fct_market_snapshot_daily
        UNION ALL
        SELECT
            'core.fct_global_market_snapshot',
            'Core (Silver)',
            'Global market capitalization & dominance',
            '1 row / snapshot',
            count(*)
        FROM core.fct_global_market_snapshot
        UNION ALL
        SELECT
            'marts.market_health_mart',
            'Marts (Gold)',
            'Macro market health KPIs & trend',
            '1 row / snapshot',
            count(*)
        FROM marts.market_health_mart
        UNION ALL
        SELECT
            'marts.top_movers_mart',
            'Marts (Gold)',
            'Latest price change & volume spikes',
            '1 row / coin (latest)',
            count(*)
        FROM marts.top_movers_mart
        UNION ALL
        SELECT
            'marts.coin_performance_mart',
            'Marts (Gold)',
            'Rolling returns (7/30/90d), volatility & drawdown',
            '1 row / coin / ngày',
            count(*)
        FROM marts.coin_performance_mart
        ORDER BY layer, row_count DESC
    """
    return _safe_query(sql)
