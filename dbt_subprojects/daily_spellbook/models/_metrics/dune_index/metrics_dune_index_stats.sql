{{ config(
        schema = 'metrics'
        , alias = 'dune_index_stats'
        )
}}

with source as (
    -- daily dune index per blockchain
    select
        blockchain
        , block_date
        , fees_index
        , transfers_index
        , tx_index
        , dune_index
    from
        {{ ref('metrics_dune_index_daily') }}
), current_day as (
    select
        blockchain
        , dune_index as last_1_days_dune_index
    from
        source
    where
        block_date >= date_trunc('day', now()) - interval '1' day
        and block_date < date_trunc('day', now())
), previous_day as (
    select
        blockchain
        , dune_index as previous_1_days_dune_index
    from
        source
    where
        block_date >= date_trunc('day', now()) - interval '2' day
        and block_date < date_trunc('day', now()) - interval '1' day
), total_current_day_dune_index as (
    select
        sum(last_1_days_dune_index) AS total_cross_chain_last_1_days_dune_index
    from
        current_day
), daily_stats as (
    select
        c.blockchain
        , c.last_1_days_dune_index as last_1_days_dune_index_contribution
        , c.last_1_days_dune_index / t.total_cross_chain_last_1_days_dune_index as last_1_days_dune_index_contribution_percent
        , t.total_cross_chain_last_1_days_dune_index
        , p.previous_1_days_dune_index as previous_1_days_dune_index_contribution
        , (c.last_1_days_dune_index - coalesce(p.previous_1_days_dune_index, 0)) / coalesce(p.previous_1_days_dune_index, 1) AS daily_percent_change
    from
        current_day as c
    left join previous_day as p
        on c.blockchain = p.blockchain
    inner join total_current_day_dune_index as t
        on 1 = 1
), current_week as (
    select
        blockchain
        , avg(dune_index) as last_7_days_dune_index
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
        , avg(dune_index) as previous_7_days_dune_index
    from
        source
    where
        block_date >= date_trunc('day', now()) - interval '14' day
        and block_date < date_trunc('day', now()) - interval '7' day
    group by
        blockchain
), total_current_week_dune_index as (
    select
        sum(last_7_days_dune_index) AS total_cross_chain_last_7_days_dune_index
    from
        current_week
), weekly_stats as (
    select
        c.blockchain
        , c.last_7_days_dune_index as last_7_days_dune_index_contribution
        , c.last_7_days_dune_index / t.total_cross_chain_last_7_days_dune_index as last_7_days_dune_index_contribution_percent
        , t.total_cross_chain_last_7_days_dune_index
        , p.previous_7_days_dune_index as previous_7_days_dune_index_contribution
        , (c.last_7_days_dune_index - coalesce(p.previous_7_days_dune_index, 0)) / coalesce(p.previous_7_days_dune_index, 1) AS weekly_percent_change
    from
        current_week as c
    left join previous_week as p
        on c.blockchain = p.blockchain
    inner join total_current_week_dune_index as t
        on 1 = 1
), current_month as (
    select
        blockchain
        , avg(dune_index) as last_30_days_dune_index
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
        , avg(dune_index) as previous_30_days_dune_index
    from
        source
    where
        block_date >= date_trunc('day', now()) - interval '60' day
        and block_date < date_trunc('day', now()) - interval '30' day
    group by
        blockchain
), total_current_month_dune_index as (
    select
        sum(last_30_days_dune_index) AS total_cross_chain_last_30_days_dune_index
    from
        current_month
), monthly_stats as (
    select
        c.blockchain
        , c.last_30_days_dune_index as last_30_days_dune_index_contribution
        , c.last_30_days_dune_index / t.total_cross_chain_last_30_days_dune_index as last_30_days_dune_index_contribution_percent
        , t.total_cross_chain_last_30_days_dune_index
        , p.previous_30_days_dune_index as previous_30_days_dune_index_contribution
        , (c.last_30_days_dune_index - coalesce(p.previous_30_days_dune_index, 0)) / coalesce(p.previous_30_days_dune_index, 1) AS monthly_percent_change
    from
        current_month as c
    left join previous_month as p
        on c.blockchain = p.blockchain
    inner join total_current_month_dune_index as t
        on 1 = 1
)
select
    d.blockchain
    , d.last_1_days_dune_index_contribution
    , d.last_1_days_dune_index_contribution_percent
    , d.total_cross_chain_last_1_days_dune_index
    , d.previous_1_days_dune_index_contribution
    , d.daily_percent_change
    , w.last_7_days_dune_index_contribution
    , w.last_7_days_dune_index_contribution_percent
    , w.total_cross_chain_last_7_days_dune_index
    , w.previous_7_days_dune_index_contribution
    , w.weekly_percent_change
    , m.last_30_days_dune_index_contribution
    , m.last_30_days_dune_index_contribution_percent
    , m.total_cross_chain_last_30_days_dune_index
    , m.previous_30_days_dune_index_contribution
    , m.monthly_percent_change
from
    daily_stats as d
inner join weekly_stats as w
    on d.blockchain = w.blockchain
inner join monthly_stats as m
    on d.blockchain = m.blockchain
