select
    coin_id,
    api_last_updated,
    fetched_at,
    market_cap_rank,
    lag(market_cap_rank) over (partition by coin_id order by api_last_updated) as previous_rank,
    lag(market_cap_rank) over (partition by coin_id order by api_last_updated) - market_cap_rank as rank_change
    -- dương = rank tăng hạng (số nhỏ hơn = tốt hơn), âm = tụt hạng
from
    {{ ref('fct_market_snapshot_hourly') }}
