{{ config(materialized='table') }}

-- ─────────────────────────────────────────────────────────────────────────────
-- top_movers_mart
-- Grain : 1 row = 1 coin — snapshot MỚI NHẤT (fetched_at cao nhất toàn bảng)
-- Mục đích: Cung cấp trạng thái "real-time" nhất của từng coin cho dashboard
--           Top Gainers / Losers và phát hiện Volume Spike.
--
-- JOIN LOGIC & NULL EDGE CASES:
--
-- [1] latest_snapshot (QUALIFY rn = 1):
--     Lấy đúng 1 bản ghi / coin — bản ghi có fetched_at cao nhất.
--     → grain của mart là 1 coin, không phải 1 coin × 1 giờ.
--
-- [2] rank_change (LEFT JOIN theo coin_id + fetched_at):
--     rank_change = previous_rank - current_rank (tính từ int_coin_rank_change).
--     Có thể NULL ở lần fetch đầu tiên của coin (không có previous_rank).
--     Dương = tăng hạng, Âm = tụt hạng, 0 = không đổi.
--
-- [3] volume_baseline (LEFT JOIN theo snapshot_date - 1):
--     Dùng T-1 (ngày HÔM QUA) thay vì T (hôm nay) để baseline không bị
--     ảnh hưởng bởi volume chưa đầy ngày của ngày hiện tại.
--     avg_volume_7d = trung bình volume của 7 ngày trước ngày T-1
--     (window: rows between 7 preceding and 1 preceding trong int_coin_volume_rolling_avg).
--     NULL trong ngày đầu tiên pipeline chạy (không có ngày T-1).
--     → volume_spike_ratio cũng NULL trong ngày đầu.
--
-- fetched_at vs snapshot_date:
--     fetched_at  = timestamp pipeline gọi API (giờ:phút:giây) — data freshness indicator.
--     snapshot_date = date(fetched_at) — dùng để filter theo ngày trên dashboard.
--     Cả hai đều cần thiết: fetched_at cho "last updated X minutes ago",
--     snapshot_date cho date picker / time-range filter.
-- ─────────────────────────────────────────────────────────────────────────────

with latest_snapshot as (
    select *,
        row_number() over (partition by coin_id order by api_last_updated desc) as rn
    from {{ ref('fct_market_snapshot_hourly') }}
    qualify rn = 1
),

rank_change as (
    select coin_id, api_last_updated, rank_change
    from {{ ref('int_coin_rank_change') }}
),

volume_baseline as (
    select coin_id, snapshot_date, avg_volume_7d
    from {{ ref('int_coin_volume_rolling_avg') }}
)

select
    l.coin_id,
    l.fetched_at,
    cast(l.fetched_at as date) as snapshot_date,
    l.current_price,
    l.price_change_percentage_24h,
    l.price_change_percentage_7d_in_currency,
    l.market_cap_rank,
    rc.rank_change,
    l.total_volume,
    v.avg_volume_7d,
    l.total_volume / nullif(v.avg_volume_7d, 0) as volume_spike_ratio
from latest_snapshot l
left join rank_change rc
    on l.coin_id = rc.coin_id and l.api_last_updated = rc.api_last_updated
left join volume_baseline v
    -- Join với ngày HÔM QUA (T-1) để dùng baseline không bị ảnh hưởng bởi
    -- volume chưa đầy của ngày hiện tại. NULL trong ngày đầu pipeline.
    on l.coin_id = v.coin_id
    and cast(l.fetched_at as date) - interval 1 day = v.snapshot_date
