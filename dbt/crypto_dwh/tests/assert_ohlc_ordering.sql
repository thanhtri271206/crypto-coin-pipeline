select
    snapshot_id,
    coin_id,
    snapshot_date,
    open,
    high,
    low,
    close
from {{ ref('fct_market_snapshot_daily') }}
where
    high < open
    or high < close
    or high < low
    or low > open
    or low > close
    or low > high
