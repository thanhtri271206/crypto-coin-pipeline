{{
  config(
    materialized = 'table',
  )
}}

-- ─────────────────────────────────────────────────────────────────────────────
-- int_coin_rolling_metrics
-- Grain      : 1 row = 1 coin × 1 ngày (coin_id, snapshot_date)
-- Source     : fct_market_snapshot_daily
-- Materialized: TABLE (override global 'view' default cho intermediate layer)
--
-- LÝ DO DÙNG TABLE thay vì VIEW:
--   Model này chứa 3 self-join trên `daily` CTE + 2 window function frames
--   (volatility) + 1 running-max + 1 rolling-min (drawdown) — tổng 5+ scan
--   trên fct_market_snapshot_daily. Nếu để view, mỗi lần mart downstream
--   (coin_performance_mart) query sẽ re-execute toàn bộ logic này.
--   Với N coins × 365 ngày, chi phí đáng kể → materialize thành table.
--
-- LƯU Ý QUAN TRỌNG — "pipeline-start peak" (KHÔNG phải ATH lịch sử):
--   drawdown_pct và max_drawdown_30d tính peak bằng running-max từ ngày
--   ĐẦU TIÊN PIPELINE CHẠY, không phải ATH thực tế của coin trên toàn
--   thị trường. Ví dụ: BTC đạt ATH $73k tháng 3/2024, nhưng nếu pipeline
--   bắt đầu tháng 1/2026 khi BTC ở $95k thì peak = $95k (cao nhất pipeline
--   thấy được), không phải $73k hay bất kỳ ATH nào trước đó.
--   → Dashboard PHẢI hiển thị disclaimer: "Drawdown tính từ đỉnh kể từ
--     ngày [min_date của pipeline], không phải ATH lịch sử toàn thời gian."
-- ─────────────────────────────────────────────────────────────────────────────

with daily as (
    select
        coin_id,
        snapshot_date,
        close,
        daily_return
    from 
        {{ ref('fct_market_snapshot_daily') }}
),
returns as (
    select
        d.coin_id,
        d.snapshot_date,
        d.close,
        (d.close - d7.close) / nullif(d7.close, 0) as rolling_return_7d, -- 7 days before
        (d.close - d30.close) / nullif(d30.close, 0) as rolling_return_30d, -- 30 days before
        (d.close - d90.close) / nullif(d90.close, 0) as rolling_return_90d -- 90 days before
    from 
        daily as d
    left join daily as d7
        on d.coin_id = d7.coin_id
        and d7.snapshot_date = d.snapshot_date - interval 7 day
    left join daily as d30
        on d.coin_id = d30.coin_id
        and d30.snapshot_date = d.snapshot_date - interval 30 day
    left join daily d90
        on d.coin_id = d90.coin_id
        and d90.snapshot_date = d.snapshot_date - interval 90 day
),
volatility  as (
    select
        coin_id,
        snapshot_date,
        stddev_samp(daily_return) over (
            partition by coin_id order by snapshot_date
            range between interval '6 days' preceding and current row
        ) as volatility_7d,
        stddev_samp(daily_return) over (
            partition by coin_id order by snapshot_date
            range between interval '29 days' preceding and current row
        ) as volatility_30d
    from 
        daily
),
-- Drawdown: baseline là đỉnh giá cao nhất từ NGÀY ĐẦU PIPELINE đến hiện tại
-- ("pipeline-start peak", chạy dần theo thời gian) — tính theo cách 'underwater curve'.
-- KHÔNG phải ATH lịch sử toàn thời gian của coin (xem header comment ở trên).
drawdown_base as (
    select
        coin_id,
        snapshot_date,
        close,
        max(close) over (
            partition by coin_id order by snapshot_date
            rows between unbounded preceding and current row
        ) as pipeline_start_peak  -- đỉnh cao nhất kể từ ngày đầu pipeline, không phải ATH
    from daily
),
drawdown as (
    select
        coin_id,
        snapshot_date,
        -- drawdown_pct: độ lệch % so với pipeline_start_peak tại mỗi ngày.
        -- = 0 khi coin đang ở đỉnh cao nhất kể từ đầu pipeline.
        -- Luôn <= 0 (coin không thể cao hơn peak của chính nó).
        (close - pipeline_start_peak) / nullif(pipeline_start_peak, 0) as drawdown_pct,
        -- max_drawdown_30d: drawdown tệ nhất (min) trong cửa sổ 30 ngày gần nhất.
        -- Luôn <= drawdown_pct. NULL trong 29 ngày đầu pipeline (cửa sổ chưa đủ 30 ngày).
        min((close - pipeline_start_peak) / nullif(pipeline_start_peak, 0)) over (
            partition by coin_id order by snapshot_date
            rows between 29 preceding and current row
        ) as max_drawdown_30d
    from drawdown_base
)


select
    r.coin_id,
    r.snapshot_date,
    r.rolling_return_7d,
    r.rolling_return_30d,
    r.rolling_return_90d,
    v.volatility_7d,
    v.volatility_30d,
    dd.drawdown_pct,
    dd.max_drawdown_30d
from returns r 
join volatility v 
    on r.coin_id = v.coin_id and r.snapshot_date = v.snapshot_date
join drawdown dd
    on r.coin_id = dd.coin_id and r.snapshot_date = dd.snapshot_date
