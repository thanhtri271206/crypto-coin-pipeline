with source as (
    select *
    from {{ source('raw_coins_metadata', 'coins_metadata') }}
),

renamed as (
    select
        id as coin_id,
        symbol,
        name,
        categories,
        platforms,

        -- description/links khai báo kiểu JSON trong sources.yml
        -- -> PHẢI dùng ->> hoặc json_extract_string, dot
        -- notation (description.en) sẽ lỗi type.
        description ->> 'en' as description_en,
        json_extract_string(links, '$.homepage[0]') as homepage_url,
        json_extract_string(links, '$.blockchain_site[0]') as block_explorer_url,

        genesis_date,
        market_cap_rank,

        last_updated as api_last_updated,
        date as snapshot_date,
        strptime(
            regexp_extract(filename, 'fetched_at=([^/]+)\.json', 1),
            '%Y-%m-%dT%H-%M-%SZ'
        ) as fetched_at

    from source
)

select * from renamed