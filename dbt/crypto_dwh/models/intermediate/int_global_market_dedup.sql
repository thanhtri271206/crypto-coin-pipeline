with ranked as (
    select
        *,
        row_number() over (partition by fetched_at order by fetched_at desc) as rn
    from {{ ref('stg_global') }}
)
select
    active_cryptocurrencies,
    markets,
    total_market_cap_usd,
    total_volume_usd,
    btc_dominance_pct,
    eth_dominance_pct,
    market_cap_change_pct_24h,
    volume_change_pct_24h,
    api_last_updated,
    snapshot_date,
    fetched_at
from
    ranked
where
    rn = 1