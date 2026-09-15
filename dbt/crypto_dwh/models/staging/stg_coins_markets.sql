{{ config(
    materialized='incremental',
    unique_key=['coin_id', 'fetched_at'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- ─────────────────────────────────────────────────────────────────────────────
-- stg_coins_markets — Incremental staging từ S3 raw JSON
--
-- Materialization: incremental (delete+insert)
-- Grain         : 1 row = 1 coin tại 1 lần fetch (coin_id, fetched_at)
-- Unique key    : ['coin_id', 'fetched_at']
--
-- INCREMENTAL STRATEGY — delete+insert với lookback 2 ngày:
--   - delete+insert: xóa record có cùng unique_key rồi insert lại
--     → idempotent khi pipeline re-run nhiều lần trong cùng ngày
--   - Lookback 2 ngày: bao gồm ngày hiện tại + 1 ngày đệm để handle
--     timezone edge cases (UTC vs local time) và late-arriving tasks.
--     ingest_market_snapshot_dag chạy @hourly, không có catchup →
--     các giờ bị miss là mất thật (API real-time only), không cần lookback dài hơn.
--
-- LƯU Ý — S3 scan behavior với read_json_auto:
--   read_json_auto KHÔNG hỗ trợ Hive partition pruning (khác với read_parquet).
--   WHERE date >= watermark áp dụng SAU khi tất cả files được list từ S3.
--   Lợi ích thực tế: giảm số rows DuckDB cần process (CPU/memory), và
--   staging trở thành DuckDB persistent table → downstream queries không
--   còn phải round-trip qua S3 network mỗi lần.
--
-- FULL-REFRESH: chạy `dbt build --full-refresh -s stg_coins_markets` để
--   reset và load lại toàn bộ history từ S3.
-- ─────────────────────────────────────────────────────────────────────────────

with source as (
    select *
    from {{ source('raw_coins_markets', 'coins_markets') }}
    {% if is_incremental() %}
    -- snapshot_date là Hive partition column (VARCHAR 'YYYY-MM-DD' từ read_json_auto).
    -- Cast về DATE trước khi trừ interval, rồi cast lại VARCHAR để so sánh đúng type.
    where date >= (
        select (max(snapshot_date)::date - interval '2 days')
        from {{ this }}
    )
    {% endif %}
),
renamed as(
    select
        id as coin_id,
        symbol,
        name,
        cast(current_price as double) as current_price,
        cast(market_cap as double) as market_cap,
        cast(market_cap_rank as bigint) as market_cap_rank,
        cast(total_volume as double) as total_volume,
        cast(high_24h as double) as high_24h,
        cast(low_24h as double) as low_24h,
        cast(price_change_24h as double) as price_change_24h,
        cast(price_change_percentage_24h as double) as price_change_percentage_24h,
        price_change_percentage_7d_in_currency,
        cast(circulating_supply as double) as circulating_supply,
        cast(total_supply as double) as total_supply,
        cast(max_supply as double) as max_supply,
        last_updated as api_last_updated,
        date as snapshot_date,
        strptime(
            regexp_extract(filename, 'fetched_at=([^/]+)\.json', 1),
            '%Y-%m-%dT%H-%M-%SZ'
        ) as fetched_at
    from
        source
)

select * from renamed
