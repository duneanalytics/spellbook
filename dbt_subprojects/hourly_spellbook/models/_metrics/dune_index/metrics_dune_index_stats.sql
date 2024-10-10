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
        , ((fees_index + transfers_index + tx_index) / 3) as last_1_days_dune_index_contribution
        , ((fees_index + transfers_index + tx_index) / 3) / dune_index as last_1_days_dune_index_contribution_percent
        , dune_index as last_1_days_dune_index
    from
        source
    where
        block_date >= date_trunc('day', now()) - interval '1' day
        and block_date < date_trunc('day', now())
), previous_day as (
    select
        blockchain
        , ((fees_index + transfers_index + tx_index) / 3) as previous_1_days_dune_index_contribution
        , ((fees_index + transfers_index + tx_index) / 3) / dune_index as previous_1_days_dune_index_contribution_percent
        , dune_index as previous_1_days_dune_index
    from
        source
    where
        block_date >= date_trunc('day', now()) - interval '2' day
        and block_date < date_trunc('day', now()) - interval '1' day
), daily_stats as (
    select
        c.blockchain
        , c.last_1_days_dune_index_contribution
        , c.last_1_days_dune_index_contribution_percent
        , c.last_1_days_dune_index
        , p.previous_1_days_dune_index_contribution
        , p.previous_1_days_dune_index_contribution_percent
        , p.previous_1_days_dune_index
        , ((c.last_1_days_dune_index - coalesce(p.previous_1_days_dune_index, 0)) / coalesce(p.previous_1_days_dune_index, 1)) * 100 AS daily_percent_change
    from
        current_day as c
    left join previous_day as p
        on c.blockchain = p.blockchain
), current_week as (
    select
        blockchain
        , ((avg(fees_index) + avg(transfers_index) + avg(tx_index)) / 3) as last_7_days_dune_index_contribution
        , ((avg(fees_index) + avg(transfers_index) + avg(tx_index)) / 3) / avg(dune_index) as last_7_days_dune_index_contribution_percent
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
        , ((avg(fees_index) + avg(transfers_index) + avg(tx_index)) / 3) as previous_7_days_dune_index_contribution
        , ((avg(fees_index) + avg(transfers_index) + avg(tx_index)) / 3) / avg(dune_index) as previous_7_days_dune_index_contribution_percent
        , avg(dune_index) as previous_7_days_dune_index
    from
        source
    where
        block_date >= date_trunc('day', now()) - interval '14' day
        and block_date < date_trunc('day', now()) - interval '7' day
    group by
        blockchain
), weekly_stats as (
    select
        c.blockchain
        , c.last_7_days_dune_index_contribution
        , c.last_7_days_dune_index_contribution_percent
        , p.previous_7_days_dune_index_contribution
        , p.previous_7_days_dune_index_contribution_percent
        , ((c.last_7_days_dune_index_contribution - coalesce(p.previous_7_days_dune_index_contribution, 0)) / coalesce(p.previous_7_days_dune_index_contribution, 1)) * 100 AS weekly_percent_change
    from
        current_week as c
    left join previous_week as p
        on c.blockchain = p.blockchain
), current_month as (
    select
        blockchain
        , ((avg(fees_index) + avg(transfers_index) + avg(tx_index)) / 3) as last_30_days_dune_index_contribution
        , ((avg(fees_index) + avg(transfers_index) + avg(tx_index)) / 3) / avg(dune_index) as last_30_days_dune_index_contribution_percent
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
        , ((avg(fees_index) + avg(transfers_index) + avg(tx_index)) / 3) as previous_30_days_dune_index_contribution
        , ((avg(fees_index) + avg(transfers_index) + avg(tx_index)) / 3) / avg(dune_index) as previous_30_days_dune_index_contribution_percent
        , avg(dune_index) as previous_30_days_dune_index
    from
        source
    where
        block_date >= date_trunc('day', now()) - interval '60' day
        and block_date < date_trunc('day', now()) - interval '30' day
    group by
        blockchain
), monthly_stats as (
    select
        c.blockchain
        , c.last_30_days_dune_index_contribution
        , c.last_30_days_dune_index_contribution_percent
        , p.previous_30_days_dune_index_contribution
        , p.previous_30_days_dune_index_contribution_percent
        , ((c.last_30_days_dune_index_contribution - coalesce(p.previous_30_days_dune_index_contribution, 0)) / coalesce(p.previous_30_days_dune_index_contribution, 1)) * 100 AS monthly_percent_change
    from
        current_month as c
    left join previous_month as p
        on c.blockchain = p.blockchain
)
select
    d.blockchain
    , d.last_1_days_dune_index_contribution
    , d.last_1_days_dune_index_contribution_percent
    , d.last_1_days_dune_index
    , d.previous_1_days_dune_index_contribution
    , d.previous_1_days_dune_index_contribution_percent
    , d.previous_1_days_dune_index
    , d.daily_percent_change
    , w.last_7_days_dune_index_contribution
    , w.last_7_days_dune_index_contribution_percent
    , w.last_7_days_dune_index
    , w.previous_7_days_dune_index_contribution
    , w.previous_7_days_dune_index_contribution_percent
    , w.previous_7_days_dune_index
    , w.weekly_percent_change
    , m.last_30_days_dune_index_contribution
    , m.last_30_days_dune_index_contribution_percent
    , m.last_30_days_dune_index
    , m.previous_30_days_dune_index_contribution
    , m.previous_30_days_dune_index_contribution_percent
    , m.previous_30_days_dune_index
    , m.monthly_percent_change
from
    daily_stats as d
inner join weekly_stats as w
    on d.blockchain = w.blockchain
inner join monthly_stats as m
    on d.blockchain = m.blockchain