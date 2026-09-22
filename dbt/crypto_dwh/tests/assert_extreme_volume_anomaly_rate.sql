{{ config(severity = 'warn') }}

-- Singular test: Warn if more than 20% of coins in the latest snapshot have extreme volume anomalies.
-- Flags potential API data format drift or macro market events requiring DE attention.

with latest_mart as (
    select
        fetched_at,
        count(*) as total_coins,
        count(case when is_extreme_volume_anomaly = true then 1 end) as anomaly_count
    from {{ ref('top_movers_mart') }}
    group by fetched_at
)

select
    fetched_at,
    total_coins,
    anomaly_count,
    (anomaly_count * 1.0 / nullif(total_coins, 0)) as anomaly_rate
from latest_mart
where (anomaly_count * 1.0 / nullif(total_coins, 0)) > 0.20
