select
    coin_id,
    snapshot_date,
    volume_24h_rolling,
    avg(volume_24h_rolling) over (
        partition by coin_id order by snapshot_date
        rows between 7 preceding and 1 preceding  -- không tính chính ngày hôm nay vào baseline
    ) as avg_volume_7d
from {{ ref('fct_market_snapshot_daily') }}