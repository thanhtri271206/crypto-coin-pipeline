with rolling_stats as (
    select
        coin_id,
        snapshot_date,
        volume_24h_rolling,
        -- Backward-compatible: 7-day rolling average
        avg(volume_24h_rolling) over (
            partition by coin_id order by snapshot_date
            rows between 7 preceding and 1 preceding  -- does not include today
        ) as avg_volume_7d,

        -- Sample statistics on 14-day rolling window (T-14 to T-1)
        count(volume_24h_rolling) over (
            partition by coin_id order by snapshot_date
            rows between 14 preceding and 1 preceding
        ) as sample_count_14d,

        avg(volume_24h_rolling) over (
            partition by coin_id order by snapshot_date
            rows between 14 preceding and 1 preceding
        ) as avg_volume_14d,

        -- Quartiles Q1 (25%) and Q3 (75%)
        quantile_cont(volume_24h_rolling, 0.25) over (
            partition by coin_id order by snapshot_date
            rows between 14 preceding and 1 preceding
        ) as q1_volume_14d,

        quantile_cont(volume_24h_rolling, 0.75) over (
            partition by coin_id order by snapshot_date
            rows between 14 preceding and 1 preceding
        ) as q3_volume_14d
    from {{ ref('fct_market_snapshot_daily') }}
),

fences as (
    select
        coin_id,
        snapshot_date,
        volume_24h_rolling,
        avg_volume_7d,
        avg_volume_14d,
        q1_volume_14d,
        q3_volume_14d,
        (q3_volume_14d - q1_volume_14d) as iqr_volume_14d,

        -- Moderate Spike Upper Fence (k = 1.5):
        -- Cold Start check: return NULL if sample_count < 7.
        -- Bounds Collapse floor: ensure at least 1.5x avg_volume_14d (protects stablecoins where IQR=0).
        case
            when sample_count_14d < 7 then null
            else greatest(
                q3_volume_14d + 1.5 * (q3_volume_14d - q1_volume_14d),
                avg_volume_14d * 1.5
            )
        end as volume_upper_fence_moderate,

        -- Extreme Anomaly Upper Fence (k = 3.0):
        -- For DE Alerting, ensure floor of at least 2.5x avg_volume_14d.
        case
            when sample_count_14d < 7 then null
            else greatest(
                q3_volume_14d + 3.0 * (q3_volume_14d - q1_volume_14d),
                avg_volume_14d * 2.5
            )
        end as volume_upper_fence_extreme
    from rolling_stats
)

select
    coin_id,
    snapshot_date,
    volume_24h_rolling,
    avg_volume_7d,
    avg_volume_14d,
    q1_volume_14d,
    q3_volume_14d,
    iqr_volume_14d,
    volume_upper_fence_moderate,
    volume_upper_fence_extreme
from fences

