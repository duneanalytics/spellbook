{{ config(
        schema = 'metrics'
        , alias = 'gas_fees_stats'
        )
}}

with source as (
    -- daily aggregation of gas fees per blockchain
    select
        blockchain
        , block_date
        , gas_fees_usd
    from
        {{ ref('metrics_gas_fees_daily') }}
), current_day as (
    select
        blockchain
        , gas_fees_usd as last_1_days_gas_fees_usd
    from
        source
    where
        block_date >= date_trunc('day', now()) - interval '1' day
        and block_date < date_trunc('day', now())
), previous_day as (
    select
        blockchain
        , gas_fees_usd as previous_1_days_gas_fees_usd
    from
        source
    where
        block_date >= date_trunc('day', now()) - interval '2' day
        and block_date < date_trunc('day', now()) - interval '1' day
), total_current_day_gas_fees as (
    select
        sum(last_1_days_gas_fees_usd) AS total_cross_chain_last_1_days_gas_fees_usd
    from
        current_day
), daily_stats as (
    select
        c.blockchain
        , c.last_1_days_gas_fees_usd
        , p.previous_1_days_gas_fees_usd
        , ((c.last_1_days_gas_fees_usd - coalesce(p.previous_1_days_gas_fees_usd, 0)) / coalesce(p.previous_1_days_gas_fees_usd, 1)) * 100 AS daily_percent_change
        , t.total_cross_chain_last_1_days_gas_fees_usd
        , (c.last_1_days_gas_fees_usd / t.total_cross_chain_last_1_days_gas_fees_usd) * 100 AS percent_total_last_1_days_fees_usd
    from
        current_day as c
    left join previous_day as p
        on c.blockchain = p.blockchain
    inner join total_current_day_gas_fees as t
        on 1 = 1
), current_week as (
    select
        blockchain
        , sum(gas_fees_usd) as last_7_days_gas_fees_usd
    from
        source
    where
        block_date >= date_trunc('day', now()) - interval '7' day
        and block_date < date_trunc('day', now())
    group by
        blockchain
), previous_week as (
    select
        blockchain
        , sum(gas_fees_usd) as previous_7_days_gas_fees_usd
    from
        source
    where
        block_date >= date_trunc('day', now()) - interval '14' day
        and block_date < date_trunc('day', now()) - interval '7' day
    group by
        blockchain
), total_current_week_gas_fees as (
    select
        sum(last_7_days_gas_fees_usd) AS total_cross_chain_last_7_days_gas_fees_usd
    from
        current_week
), weekly_stats as (
    select
        c.blockchain
        , c.last_7_days_gas_fees_usd
        , p.previous_7_days_gas_fees_usd
        , ((c.last_7_days_gas_fees_usd - coalesce(p.previous_7_days_gas_fees_usd, 0)) / coalesce(p.previous_7_days_gas_fees_usd, 1)) * 100 AS weekly_percent_change
        , t.total_cross_chain_last_7_days_gas_fees_usd
        , (c.last_7_days_gas_fees_usd / t.total_cross_chain_last_7_days_gas_fees_usd) * 100 AS percent_total_last_7_days_gas_fees_usd
    from
        current_week as c
    left join previous_week as p
        on c.blockchain = p.blockchain
    inner join total_current_week_gas_fees as t
        on 1 = 1
), current_month as (
    select
        blockchain
        , sum(gas_fees_usd) as last_30_days_gas_fees_usd
    from
        source
    where
        block_date >= date_trunc('day', now()) - interval '30' day
        and block_date < date_trunc('day', now())
    group by
        blockchain
), previous_month as (
    select
        blockchain
        , sum(gas_fees_usd) as previous_30_days_gas_fees_usd
    from
        source
    where
        block_date >= date_trunc('day', now()) - interval '60' day
        and block_date < date_trunc('day', now()) - interval '30' day
    group by
        blockchain
), total_current_month_gas_fees as (
    select
        sum(last_30_days_gas_fees_usd) AS total_cross_chain_last_30_days_gas_fees_usd
    from
        current_month
), monthly_stats as (
    select
        c.blockchain
        , c.last_30_days_gas_fees_usd
        , p.previous_30_days_gas_fees_usd
        , ((c.last_30_days_gas_fees_usd - coalesce(p.previous_30_days_gas_fees_usd, 0)) / coalesce(p.previous_30_days_gas_fees_usd, 1)) * 100 AS monthly_percent_change
        , t.total_cross_chain_last_30_days_gas_fees_usd
        , (c.last_30_days_gas_fees_usd / t.total_cross_chain_last_30_days_gas_fees_usd) * 100 AS percent_total_last_30_days_gas_fees_usd
    from
        current_month as c
    left join previous_month as p
        on c.blockchain = p.blockchain
    inner join total_current_month_gas_fees as t
        on 1 = 1
)
select
    d.blockchain
    , d.last_1_days_gas_fees_usd
    , d.previous_1_days_gas_fees_usd
    , d.daily_percent_change
    , d.total_cross_chain_last_1_days_gas_fees_usd
    , d.percent_total_last_1_days_fees_usd
    , w.last_7_days_gas_fees_usd
    , w.previous_7_days_gas_fees_usd
    , w.weekly_percent_change
    , w.total_cross_chain_last_7_days_gas_fees_usd
    , w.percent_total_last_7_days_gas_fees_usd
    , m.last_30_days_gas_fees_usd
    , m.previous_30_days_gas_fees_usd
    , m.monthly_percent_change
    , m.total_cross_chain_last_30_days_gas_fees_usd
    , m.percent_total_last_30_days_gas_fees_usd
    , m.last_30_days_gas_fees_usd * 12 as gas_fees_usd_run_rate
from
    daily_stats as d
inner join weekly_stats as w
    on d.blockchain = w.blockchain
inner join monthly_stats as m
    on d.blockchain = m.blockchain