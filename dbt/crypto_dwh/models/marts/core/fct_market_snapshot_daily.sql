{{ config(
    materialized='incremental',
    unique_key='snapshot_id',
    incremental_strategy='delete+insert'
) }}

with hourly as (
 
    select
        coin_id,
        cast(fetched_at as date) as snapshot_date,
        fetched_at,
        current_price,
        total_volume
    from {{ ref('fct_market_snapshot_hourly') }}
    {% if is_incremental() %}
    -- Lookback 1 day to fully reprocess any day that was incomplete
    -- on the previous run (e.g. pipeline ran mid-day).
    where cast(fetched_at as date) >= (select max(snapshot_date) - interval 1 day from {{ this }})
    {% endif %}
 
),
 
ranked as (
 
    select
        *,
        row_number() over (
            partition by coin_id, snapshot_date order by fetched_at asc
        ) as rn_first,
        row_number() over (
            partition by coin_id, snapshot_date order by fetched_at desc
        ) as rn_last
    from hourly
 
)
 
select
    {{ dbt_utils.generate_surrogate_key(['coin_id', 'snapshot_date']) }} as snapshot_id,
    coin_id,
    snapshot_date,
    strftime(snapshot_date, '%Y%m%d') as snapshot_date_id,
    max(case when rn_first = 1 then current_price end) as open,
    max(current_price) as high,
    min(current_price) as low,
    max(case when rn_last = 1 then current_price end) as close,
    max(case when rn_last = 1 then total_volume end) as volume_24h_rolling,
    (
        max(case when rn_last = 1 then current_price end)
        - max(case when rn_first = 1 then current_price end)
    ) / nullif(max(case when rn_first = 1 then current_price end), 0) as daily_return
    -- daily_return = (close - open) / open

from ranked
group by coin_id, snapshot_date