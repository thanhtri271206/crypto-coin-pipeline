with ranked as (
    select
        *,
        row_number() over (partition by coin_id, ts_ms order by fetched_at desc) as rn
    from
        {{ ref('stg_market_chart') }}
)

select
    coin_id,
    ts_ms,
    price_usd,
    fetched_at
from
    ranked
where rn = 1
