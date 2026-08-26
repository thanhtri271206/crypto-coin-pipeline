with source as (
    select 
        *
    from {{ source('raw_global', 'global') }}
),
renamed as (
    select 
        data.active_cryptocurrencies,
        data.markets,
        data.total_market_cap.usd as total_market_cap_usd,
        data.total_volume.usd as total_volume_usd,
        data.market_cap_percentage.btc as btc_dominance_pct,
        data.market_cap_percentage.eth as eth_dominance_pct,
        data.market_cap_change_percentage_24h_usd as market_cap_change_pct_24h,
        data.volume_change_percentage_24h_usd as volume_change_pct_24h,
        to_timestamp(data.updated_at) as api_last_updated,
        date as snapshot_date,
        strptime(
            regexp_extract(filename, 'fetched_at=([^/]+)\.json', 1),
            '%Y-%m-%dT%H-%M-%SZ'
        ) as fetched_at
    from 
        source
)

select * from renamed