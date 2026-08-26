{{ config(
    materialized='incremental',
    unique_key='snapshot_id',
    incremental_strategy='delete+insert'
) }}

-- ─────────────────────────────────────────────────────────────────────────────
-- fct_global_market_snapshot
-- Grain      : 1 row = 1 lần fetch CoinGecko /global endpoint (fetched_at duy nhất)
-- Source     : int_global_market_dedup (đã dedup để xử lý pipeline re-run)
-- Strategy   : incremental / delete+insert theo unique_key = snapshot_id
--
-- INCREMENTAL FILTER — lookback 1 giờ thay vì strict "fetched_at > max":
--   Nếu dùng strict (>), một fetch bị lỗi và không lưu được vào DuckDB
--   sẽ bị BỎ QUA ở lần chạy tiếp theo vì max(fetched_at) đã nhảy qua nó.
--   Ví dụ: fetch lúc 14:00 fail → max vẫn là 13:00 → lần chạy 15:00 dùng
--   filter > 13:00 → fetch 14:00 được reprocess đúng cách ✓
--   Lookback 1 giờ đủ an toàn vì /global thường chạy mỗi 1-4 giờ.
--   delete+insert đảm bảo không bị duplicate nếu snapshot_id đã tồn tại.
--
-- snapshot_date_id tính inline (KHÔNG join dim_time) để tránh:
--   - Fan-out nếu dim_time chưa cover date hiện tại (edge-case khi full-refresh)
--   - Dependency cycle giữa fct_global_market_snapshot và dim_time
-- ─────────────────────────────────────────────────────────────────────────────

select
    {{ dbt_utils.generate_surrogate_key(['fetched_at']) }} as snapshot_id,
    active_cryptocurrencies,
    markets,
    total_market_cap_usd,
    total_volume_usd,
    btc_dominance_pct,
    eth_dominance_pct,
    market_cap_change_pct_24h,
    volume_change_pct_24h,
    api_last_updated,
    fetched_at,
    strftime(snapshot_date, '%Y%m%d') as snapshot_date_id
from {{ ref('int_global_market_dedup') }}

{% if is_incremental() %}
-- Lookback 1 giờ: reprocess các fetch trong window gần nhất để handle
-- trường hợp fetch bị fail và không được lưu ở lần chạy trước.
-- delete+insert theo snapshot_id đảm bảo không duplicate.
where fetched_at >= (select max(fetched_at) - interval 1 hour from {{ this }})
{% endif %}

