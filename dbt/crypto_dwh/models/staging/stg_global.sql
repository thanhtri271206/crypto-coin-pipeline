{{ config(
    materialized='incremental',
    unique_key='fetched_at',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- ─────────────────────────────────────────────────────────────────────────────
-- stg_global — Incremental staging từ S3 raw JSON
--
-- Materialization: incremental (delete+insert)
-- Grain         : 1 row = 1 lần fetch /global endpoint (fetched_at)
-- Unique key    : fetched_at
--
-- INCREMENTAL STRATEGY — delete+insert với lookback 1 ngày:
--   - Lookback 1 ngày đủ cho /global (chạy @hourly, không có catchup).
--     Các snapshot bị miss khi pipeline down là mất thật (API real-time only).
--   - delete+insert idempotent: cùng fetched_at được xóa rồi insert lại
--     → pipeline re-run không tạo duplicate.
--
-- Lưu ý về downstream:
--   fct_global_market_snapshot (incremental, lookback 1 ngày) đọc từ
--   int_global_market_dedup (view) → stg_global (table này).
--   Mart table đã tích lũy toàn bộ history → staging chỉ cần cung cấp
--   dữ liệu gần nhất cho incremental run.
--
-- FULL-REFRESH: `dbt build --full-refresh -s stg_global`
-- ─────────────────────────────────────────────────────────────────────────────

with source as (
    select
        *
    from {{ source('raw_global', 'global') }}
    {% if is_incremental() %}
    -- snapshot_date là Hive partition column (VARCHAR 'YYYY-MM-DD').
    -- Lookback 1 ngày: /global chạy hourly, không cần buffer dài.
    where date >= (
        select (max(snapshot_date)::date - interval '1 day')
        from {{ this }}
    )
    {% endif %}
),
renamed as (
    select
        data.active_cryptocurrencies,
        data.markets,
        data.total_market_cap.usd as total_market_cap_usd,
        data.total_volume.usd as total_volume_usd,
        data.market_cap_percentage.btc as btc_dominance_pct,
        data.market_cap_percentage.eth as eth_dominance_pct,
        data.market_cap_change_percentage_24h_usd as market_cap_change_pct_24h,
        data.volume_change_percentage_24h_usd as volume_change_pct_24h,
        to_timestamp(data.updated_at) as api_last_updated,
        date as snapshot_date,
        strptime(
            regexp_extract(filename, 'fetched_at=([^/]+)\.json', 1),
            '%Y-%m-%dT%H-%M-%SZ'
        ) as fetched_at
    from
        source
)

select * from renamed
