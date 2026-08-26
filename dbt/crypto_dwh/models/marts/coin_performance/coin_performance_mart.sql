{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['coin_id', 'snapshot_date']) }} as performance_id,
    coin_id,
    snapshot_date,
    strftime(snapshot_date, '%Y%m%d')  as snapshot_date_id,
    rolling_return_7d,
    rolling_return_30d,
    rolling_return_90d,
    volatility_7d,
    volatility_30d,
    drawdown_pct,
    max_drawdown_30d
from {{ ref('int_coin_rolling_metrics') }}