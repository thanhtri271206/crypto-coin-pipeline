with source as (
    select *
    from {{ source('raw_market_chart', 'market_chart') }}
),
with_keys as (
    select
        *,
        regexp_extract(filename, 'raw/coins/([^/]+)/market_chart/', 1) as coin_id,
        strptime(
            regexp_extract(filename, 'fetched_at=([^/]+)\.json', 1),
            '%Y-%m-%dT%H-%M-%SZ'
        ) as fetched_at
    from source
),

-- unnest RIÊNG 1 lần thành cột trung gian trước khi index [1]/[2] — tránh
-- gọi unnest() nhiều lần trên cùng 1 cột trong cùng 1 select list (dễ gây
-- lệch cardinality giữa các cột).
unnested as (
    select
        coin_id,
        fetched_at,
        date as snapshot_date,
        unnest(prices) as price_point
    from with_keys
)

select
    coin_id,
    fetched_at,
    snapshot_date,
    price_point[1]::bigint  as ts_ms,
    price_point[2]::double  as price_usd
from unnested