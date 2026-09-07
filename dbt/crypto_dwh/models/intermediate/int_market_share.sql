with
-- Với mỗi (coin_id, snapshot_hour), chọn 1 bản ghi fetched_at mới nhất.
-- Tránh SUM cộng dồn market_cap khi có nhiều fetch trong cùng 1 giờ.
top10_per_hour as (
    select
        coin_id,
        market_cap,
        date_trunc('hour', fetched_at) as snapshot_hour,
        row_number() over (
            partition by coin_id, date_trunc('hour', fetched_at)
            order by fetched_at desc
        ) as rn
    from {{ ref('fct_market_snapshot_hourly') }}
),

top10_agg as (
    select
        snapshot_hour,
        sum(market_cap) as top10_market_cap_usd
    from top10_per_hour
    where rn = 1  -- chỉ lấy fetch mới nhất trong mỗi giờ, mỗi coin
    group by 1
),

global_hourly as (
    select
        *,
        date_trunc('hour', fetched_at) as snapshot_hour
    from {{ ref('fct_global_market_snapshot') }}
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
