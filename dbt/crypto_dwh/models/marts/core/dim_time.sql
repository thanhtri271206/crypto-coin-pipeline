with date_bounds as (
    select
        coalesce(
            min(cast(to_timestamp(ts_ms / 1000) as date)),
            cast('2020-01-01' as date)
        ) as start_date,
        cast(current_date + interval 30 day as date) as end_date
    from {{ ref('int_market_snapshot_dedup') }}
),

spine as (
    select
        unnest(
            generate_series(
                (select start_date from date_bounds),
                (select end_date from date_bounds),
                interval 1 day
            )
        )::date as date_day
),

extracted as (
    select
        strftime(date_day, '%Y%m%d') as date_id,
        cast(date_day as date) as date,
        dayofweek(date_day) as day_of_week,
        week(date_day) as week,
        month(date_day) as month,
        quarter(date_day) as quarter,
        year(date_day) as year,
        day(date_day) as day_of_month,
        weekofyear(date_day) as week_of_year,
        -- binary flags
        case when dayofweek(date_day) in (0,6) then 1 else 0 end as is_weekend,
        case when month(date_day) in (12,1,2) then 1 else 0 end as is_in_q1,
        case when month(date_day) in (3,4,5) then 1 else 0 end as is_in_q2,
        case when month(date_day) in (6,7,8) then 1 else 0 end as is_in_q3,
        case when month(date_day) in (9,10,11) then 1 else 0 end as is_in_q4
    from spine
)

select * from extracted
