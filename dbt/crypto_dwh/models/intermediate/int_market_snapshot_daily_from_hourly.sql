-- int_market_snapshot_daily_from_hourly.sql
--
-- Nguồn "chính xác" — có open/high/low/close THẬT từ dữ liệu intraday
-- (hourly). Ưu tiên nguồn này khi trùng ngày với nguồn seed lịch sử.

with hourly as (

    select
        coin_id,
        cast(fetched_at as date) as snapshot_date,
        fetched_at,
        current_price,
        total_volume
    from {{ ref('fct_market_snapshot_hourly') }}

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
    coin_id,
    snapshot_date,
    max(case when rn_first = 1 then current_price end) as open,
    max(current_price)                                    as high,
    min(current_price)                                    as low,
    max(case when rn_last = 1 then current_price end)    as close,
    max(case when rn_last = 1 then total_volume end)      as volume_24h_rolling,
    (
        max(case when rn_last = 1 then current_price end)
        - max(case when rn_first = 1 then current_price end)
    ) / nullif(max(case when rn_first = 1 then current_price end), 0) as daily_return
    -- daily_return = (close - open) / open
from ranked
group by coin_id, snapshot_date
