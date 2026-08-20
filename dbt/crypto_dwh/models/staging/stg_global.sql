with source as (
    select 
        *
    from {{ source('raw_global', 'global') }}
),
renamed as (
    select 
        data.active_cryptocurrencies,
        data.total_market_cap.usd as total_market_cap_usd,
        data.markets,
        data.total_volume.usd as total_volume_usd,
        data.market_cap_percentage.btc as btc_dominance_pct,
        date as snapshot_date,
        strptime(
            regexp_extract(filename, 'fetched_at=([^/]+)\.json', 1),
            '%Y-%m-%dT%H-%M-%SZ'
        ) as fetched_at
    from 
        source
)

select * from renamed