select 
    snapshot_id, 
    coin_id, 
    market_cap_rank
from 
    {{ ref('fct_market_snapshot_hourly') }}
where 
    market_cap_rank is not null 
    and market_cap_rank <= 0
