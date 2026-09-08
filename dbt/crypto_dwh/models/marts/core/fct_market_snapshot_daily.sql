{{ config(materialized='table') }}
-- Đổi từ incremental sang table: giờ có 2 nguồn với cursor khác nhau
-- (fetched_at bên hourly, không có tương đương bên chart-seed) — logic
-- incremental đúng cho cả 2 nguồn phức tạp hơn giá trị nó đem lại. Data
-- volume nhỏ (Top 10 coin, tối đa vài năm lịch sử) nên rebuild toàn bộ
-- vẫn đủ nhanh.

with from_hourly as (

    select *, 1 as source_priority  -- 1 = ưu tiên cao hơn (OHLC thật)
    from {{ ref('int_market_snapshot_daily_from_hourly') }}

),

from_chart_seed as (

    select *, 2 as source_priority  -- 2 = chỉ dùng khi hourly chưa có
    from {{ ref('int_market_snapshot_daily_from_chart') }}

),

unioned as (
    select * from from_hourly
    union all
    select * from from_chart_seed
),

deduped as (

    select
        *,
        row_number() over (
            partition by coin_id, snapshot_date
            order by source_priority asc
        ) as rn
    from unioned

)

select
    {{ dbt_utils.generate_surrogate_key(['coin_id', 'snapshot_date']) }} as snapshot_id,
    coin_id,
    snapshot_date,
    strftime(snapshot_date, '%Y%m%d') as snapshot_date_id,
    open,
    high,
    low,
    close,
    volume_24h_rolling,
    daily_return
from deduped
where rn = 1
