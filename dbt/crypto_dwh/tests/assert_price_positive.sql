select
    snapshot_id,
    coin_id,
    current_price,
    market_cap,
    total_volume
from
    {{ ref('fct_market_snapshot_hourly') }}
where
    current_price <= 0
    or market_cap < 0
    or total_volume < 0
