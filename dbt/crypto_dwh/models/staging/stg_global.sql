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
        data.total_market_cap.usd as total_market_cap_usd_raw,
        data.total_volume.usd as total_volume_usd_raw,
        data.market_cap_percentage.btc as btc_dominance_pct_raw,
        data.market_cap_percentage.eth as eth_dominance_pct_raw,
        data.market_cap_change_percentage_24h_usd as market_cap_change_pct_24h_raw,
        data.volume_change_percentage_24h_usd as volume_change_pct_24h_raw,
        to_timestamp(data.updated_at) at time zone 'UTC' as api_last_updated,
        date as snapshot_date,
        strptime(
            regexp_extract(filename, 'fetched_at=([^/]+)\.json', 1),
            '%Y-%m-%dT%H-%M-%SZ'
        ) as fetched_at
    from
        source
),
sanitized as (
    select
        active_cryptocurrencies,
        markets,

        -- Sanity Guard: Total market cap must be positive and < $20T
        case 
            when total_market_cap_usd_raw <= 0 or total_market_cap_usd_raw > 2e13 then null 
            else total_market_cap_usd_raw 
        end as total_market_cap_usd,

        -- Sanity Guard: Total volume cannot be negative or exceed $5T.
        -- If corrupted (e.g. quintillions due to API coin glitch), forward-fill from last clean snapshot
        -- to ensure downstream 'not_null' contract constraints are strictly preserved.
        coalesce(
            case 
                when total_volume_usd_raw between 0 and 5e12 then total_volume_usd_raw 
                else null 
            end,
            last_value(
                case when total_volume_usd_raw between 0 and 5e12 then total_volume_usd_raw end 
                ignore nulls
            ) over (
                order by fetched_at 
                rows between unbounded preceding and 1 preceding
            )
        ) as total_volume_usd,

        -- Sanity Guard: Dominance percentages strictly between 0 and 100
        case 
            when btc_dominance_pct_raw between 0 and 100 then btc_dominance_pct_raw 
            else null 
        end as btc_dominance_pct,

        case 
            when eth_dominance_pct_raw between 0 and 100 then eth_dominance_pct_raw 
            else null 
        end as eth_dominance_pct,

        -- Sanity Guard: 24h market cap change bounded [-90%, +200%]
        case 
            when market_cap_change_pct_24h_raw between -90 and 200 then market_cap_change_pct_24h_raw 
            else null 
        end as market_cap_change_pct_24h,

        -- Sanity Guard: 24h volume change bounded [-95%, +500%], nullify corrupt jumps like +11,000,000,000%
        case 
            when volume_change_pct_24h_raw between -95 and 500 then volume_change_pct_24h_raw 
            else null 
        end as volume_change_pct_24h,

        api_last_updated,
        snapshot_date,
        fetched_at
    from
        renamed
)

select * from sanitized
