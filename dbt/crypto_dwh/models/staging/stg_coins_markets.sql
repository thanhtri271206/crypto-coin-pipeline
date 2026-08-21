with source as (
    select *
    from {{ source('raw_coins_markets', 'coins_markets') }}
),
renamed as(
    select
        id as coin_id,
        symbol,
        name,
        cast(current_price as double) as current_price,
        cast(market_cap as double) as market_cap,
        cast(market_cap_rank as bigint) as market_cap_rank,
        cast(total_volume as double) as total_volume,
        cast(high_24h as double) as high_24h,
        cast(low_24h as double) as low_24h,
        cast(price_change_24h as double) as price_change_24h,
        cast(price_change_percentage_24h as double) as price_change_percentage_24h,
        cast(circulating_supply as double) as circulating_supply,
        cast(total_supply as double) as total_supply,
        cast(max_supply as double) as max_supply,
        last_updated as api_last_updated,
        date as snapshot_date,
        strptime(
            regexp_extract(filename, 'fetched_at=([^/]+)\.json', 1),
            '%Y-%m-%dT%H-%M-%SZ'
        ) as fetched_at
    from 
        source
)

select * from renamed
    
