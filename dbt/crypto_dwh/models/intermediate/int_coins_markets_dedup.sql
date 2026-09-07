with ranked as (
    select
        *,
        row_number() over (
            partition by coin_id, api_last_updated order by fetched_at desc
        ) as rn
    from {{ ref('stg_coins_markets') }}
)

select
    coin_id,
    symbol,
    name,
    current_price,
    market_cap,
    market_cap_rank,
    total_volume,
    high_24h,
    low_24h,
    price_change_24h,
    price_change_percentage_24h,
    price_change_percentage_7d_in_currency,
    circulating_supply,
    total_supply,
    max_supply,
    api_last_updated,
    snapshot_date,
    fetched_at
from ranked
where rn = 1
