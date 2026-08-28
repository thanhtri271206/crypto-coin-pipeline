-- int_market_snapshot_daily_from_chart.sql
--
-- Nguồn "seed lịch sử" — dùng để demo có đủ data mà không cần chờ pipeline
-- chạy thật 30-90 ngày. Dùng lại int_market_snapshot_dedup (đã dedupe
-- market_chart_backfill + incremental) làm nguồn.
--
-- LƯU Ý QUAN TRỌNG (giới hạn chất lượng data, không phải bug):
-- CoinGecko tự đổi granularity của market_chart theo độ dài "days" —
-- khoảng ~90 ngày gần nhất có thể có nhiều điểm/ngày (gần giống hourly),
-- nhưng xa hơn 90 ngày thường chỉ có ĐÚNG 1 điểm giá/ngày. Với những ngày
-- chỉ có 1 điểm, open/high/low/close ở model này sẽ BẰNG NHAU (không có
-- OHLC thật, chỉ có 1 mức giá đại diện cho cả ngày) — chấp nhận được cho
-- mục đích demo, nhưng cần ghi rõ trong docs/README, không nên trình bày
-- như dữ liệu OHLC đầy đủ.

with source as (
    select *
    from {{ ref('int_market_snapshot_dedup') }}
),

with_date as (
    select
        coin_id,
        to_timestamp(ts_ms / 1000)::date as snapshot_date,
        ts_ms,
        price_usd
    from source
),

ranked as (
    select
        *,
        row_number() over (
            partition by coin_id, snapshot_date order by ts_ms asc
        ) as rn_first,
        row_number() over (
            partition by coin_id, snapshot_date order by ts_ms desc
        ) as rn_last
    from with_date
)

select
    coin_id,
    snapshot_date,
    max(case when rn_first = 1 then price_usd end) as open,
    max(price_usd)                                    as high,
    min(price_usd)                                    as low,
    max(case when rn_last = 1 then price_usd end)    as close,
    cast(null as double)                              as volume_24h_rolling,  -- market_chart total_volumes chưa unnest — để trống, không suy diễn
    (
        max(case when rn_last = 1 then price_usd end)
        - max(case when rn_first = 1 then price_usd end)
    ) / nullif(max(case when rn_first = 1 then price_usd end), 0) as daily_return
    -- daily_return = (close - open) / open
from ranked
group by coin_id, snapshot_date