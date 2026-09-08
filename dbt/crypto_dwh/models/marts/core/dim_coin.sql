select
    {{ dbt_utils.generate_surrogate_key(['coin_id']) }} as dim_coin_sk,
    coin_id,
    symbol,
    name,
    categories,
    description_en,
    homepage_url,
    genesis_date,
    platforms,
    market_cap_rank as market_cap_rank_static,
    fetched_at as metadata_last_updated
from {{ ref('int_coin_metadata_latest') }}
