{{ config(
    materialized='incremental',
    unique_key='snapshot_id',
    incremental_strategy='delete+insert'
) }}

select
    {{ dbt_utils.generate_surrogate_key(['coin_id', 'fetched_at']) }} as snapshot_id,
    coin_id,
    current_price,
    market_cap,
    total_volume,
    high_24h,
    low_24h,
    price_change_24h,
    price_change_percentage_24h,
    price_change_percentage_7d_in_currency,
    market_cap_rank,
    api_last_updated,
    fetched_at,
    -- Compute date_id inline instead of joining dim_time.
    -- Avoids fan-out risk when spine doesn't cover edge-case dates.
    strftime(snapshot_date, '%Y%m%d') as snapshot_date_id
from {{ ref('int_coins_markets_dedup') }}

{% if is_incremental() %}
-- Process only fetches newer than the latest row already in the table.
-- This avoids a full scan of int_coins_markets_dedup on every run.
where fetched_at > (select max(fetched_at) from {{ this }})
{% endif %}
