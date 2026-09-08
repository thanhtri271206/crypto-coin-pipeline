{{
  config(
    materialized = 'table',
    )
}}

select
    snapshot_id,
    fetched_at,
    snapshot_date_id,
    total_market_cap_usd,
    total_volume_usd,
    btc_dominance_pct,
    eth_dominance_pct,
    top10_market_cap_usd,
    top10_market_share_pct,
    market_cap_change_pct_24h,
    volume_change_pct_24h
from
    {{ ref('int_market_share') }}
