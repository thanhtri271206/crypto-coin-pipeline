with ranked as (
    select
        *,
        row_number() over (
            partition by coin_id order by api_last_updated desc
        ) as rn
    from {{ ref('stg_coin_metadata') }}
)

select
    coin_id,
    symbol,
    name,
    categories,
    description_en,
    homepage_url,
    genesis_date,
    platforms,
    market_cap_rank,
    fetched_at
from ranked
where rn = 1
