{{ config(
        schema = 'metrics'
        , alias = 'gas_fees_stats'
        )
}}

with source as (
    -- daily aggregation of gas fees per blockchain
    select
        blockchain
        , day
        , gas_spent_usd
    from
        {{ ref('metrics_gas_fees_daily') }}
), current_day as (
    select
        blockchain
        , gas_spent_usd as current_day_gas_spent_usd
    from
        source
    where
        day >= date_trunc('day', now()) - interval '1' day
        and day < date_trunc('day', now())
), previous_day as (
    select
        blockchain
        , gas_spent_usd as previous_day_gas_spent_usd
    from
        source
    where
        day >= date_trunc('day', now()) - interval '2' day
        and day < date_trunc('day', now()) - interval '1' day
), total_current_day_fees as (
    select
        sum(current_day_gas_spent_usd) AS total_current_day_fees
    from
        current_day
), daily_stats as (
    select
        c.blockchain
        , c.current_day_gas_spent_usd
        , p.previous_day_gas_spent_usd
        , ((c.current_day_gas_spent_usd - coalesce(p.previous_day_gas_spent_usd, 0)) / coalesce(p.previous_day_gas_spent_usd, 1)) * 100 AS daily_percent_change
        , t.total_current_day_fees
        , (c.current_day_gas_spent_usd / t.total_current_day_fees) * 100 AS percent_of_total_current_day_fees
    from
        current_day as c
    left join previous_day as p
        on c.blockchain = p.blockchain
    inner join total_current_day_fees as t
        on 1 = 1
), current_week as (
    select
        blockchain
        , sum(gas_spent_usd) as current_week_gas_spent_usd
    from
        source
    where
        day >= date_trunc('day', now()) - interval '7' day
        and day < date_trunc('day', now())
    group by
        blockchain
), previous_week as (
    select
        blockchain
        , sum(gas_spent_usd) as previous_week_gas_spent_usd
    from
        source
    where
        day >= date_trunc('day', now()) - interval '14' day
        and day < date_trunc('day', now()) - interval '7' day
    group by
        blockchain
), total_current_week_fees as (
    select
        sum(current_week_gas_spent_usd) AS total_current_week_fees
    from
        current_week
), weekly_stats as (
    select
        c.blockchain
        , c.current_week_gas_spent_usd
        , p.previous_week_gas_spent_usd
        , ((c.current_week_gas_spent_usd - coalesce(p.previous_week_gas_spent_usd, 0)) / coalesce(p.previous_week_gas_spent_usd, 1)) * 100 AS weekly_percent_change
        , t.total_current_week_fees
        , (c.current_week_gas_spent_usd / t.total_current_week_fees) * 100 AS percent_of_total_current_week_fees
    from
        current_week as c
    left join previous_week as p
        on c.blockchain = p.blockchain
    inner join total_current_week_fees as t
        on 1 = 1
), current_month as (
    select
        blockchain
        , sum(gas_spent_usd) as current_month_gas_spent_usd
    from
        source
    where
        day >= date_trunc('day', now()) - interval '30' day
        and day < date_trunc('day', now())
    group by
        blockchain
), previous_month as (
    select
        blockchain
        , sum(gas_spent_usd) as previous_month_gas_spent_usd
    from
        source
    where
        day >= date_trunc('day', now()) - interval '60' day
        and day < date_trunc('day', now()) - interval '30' day
    group by
        blockchain
), total_current_month_fees as (
    select
        sum(current_month_gas_spent_usd) AS total_current_month_fees
    from
        current_month
), monthly_stats as (
    select
        c.blockchain
        , c.current_month_gas_spent_usd
        , p.previous_month_gas_spent_usd
        , ((c.current_month_gas_spent_usd - coalesce(p.previous_month_gas_spent_usd, 0)) / coalesce(p.previous_month_gas_spent_usd, 1)) * 100 AS monthly_percent_change
        , t.total_current_month_fees
        , (c.current_month_gas_spent_usd / t.total_current_month_fees) * 100 AS percent_of_total_current_month_fees
    from
        current_month as c
    left join previous_month as p
        on c.blockchain = p.blockchain
    inner join total_current_month_fees as t
        on 1 = 1
)
select
    d.blockchain
    , d.current_day_gas_spent_usd
    , d.previous_day_gas_spent_usd
    , d.daily_percent_change
    , d.total_current_day_fees
    , d.percent_of_total_current_day_fees
    , w.current_week_gas_spent_usd
    , w.previous_week_gas_spent_usd
    , w.weekly_percent_change
    , w.total_current_week_fees
    , w.percent_of_total_current_week_fees
    , m.current_month_gas_spent_usd
    , m.previous_month_gas_spent_usd
    , m.monthly_percent_change
    , m.total_current_month_fees
    , m.percent_of_total_current_month_fees
    , m.total_current_month_fees * 12 as gas_spent_usd_run_rate
from
    daily_stats as d
inner join weekly_stats as w
    on d.blockchain = w.blockchain
inner join monthly_stats as m
    on d.blockchain = m.blockchain