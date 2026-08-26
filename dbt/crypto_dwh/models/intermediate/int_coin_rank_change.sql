select
    coin_id,
    fetched_at,
    market_cap_rank,
    lag(market_cap_rank) over(partition by coin_id order by fetched_at) as previous_rank,
    lag(market_cap_rank) over (partition by coin_id order by fetched_at) - market_cap_rank as rank_change
    -- dương = rank tăng hạng (số nhỏ hơn = tốt hơn), âm = tụt hạng
from 
    {{ ref('fct_market_snapshot_hourly') }}