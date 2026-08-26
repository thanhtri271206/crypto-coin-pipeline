with top10_agg as (
    select
        date_trunc('hour', fetched_at) as snapshot_hour,
        sum(market_cap) as top10_market_cap_usd
    from 
        {{ ref('fct_market_snapshot_hourly') }}
    group by 1
),
global_hourly as (
    select
        *,
        date_trunc('hour', fetched_at) as snapshot_hour
    from 
        {{ ref('fct_global_market_snapshot') }}
)

select
    g.*,
    t.top10_market_cap_usd,
    t.top10_market_cap_usd / nullif(g.total_market_cap_usd, 0) as top10_market_share_pct
from 
    global_hourly as g
left join 
    top10_agg as t
on g.snapshot_hour = t.snapshot_hour