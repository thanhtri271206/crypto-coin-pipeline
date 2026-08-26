select 
    snapshot_id,
    coin_id,
    fetched_at,
    CURRENT_TIMESTAMP as processed_at
from 
    {{ ref('fct_market_snapshot_hourly') }}
where 
    fetched_at > CURRENT_TIMESTAMP