select 
    snapshot_id, 
    coin_id, 
    snapshot_date, 
    open, 
    close, 
    daily_return
from 
    {{ ref('fct_market_snapshot_daily') }}
where 
    daily_return < -1.0 
    or (open = 0 and daily_return is not null)
