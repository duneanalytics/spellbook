{{ config(
        schema = 'metrics'
        , alias = 'transactions_stats'
        )
}}

with source as (
    -- daily aggregation of tx's per blockchain
    select
        blockchain
        , block_date
        , tx_count
    from
        {{ ref('metrics_transactions_daily') }}
), current_day as (
    select
        blockchain
        , tx_count as last_1_days_tx_count
    from
        source
    where
        block_date >= date_trunc('day', now()) - interval '1' day
        and block_date < date_trunc('day', now())
), previous_day as (
    select
        blockchain
        , tx_count as previous_1_days_tx_count
    from
        source
    where
        block_date >= date_trunc('day', now()) - interval '2' day
        and block_date < date_trunc('day', now()) - interval '1' day
), total_current_day_txs as (
    select
        sum(last_1_days_tx_count) AS total_cross_chain_last_1_days_tx_count
    from
        current_day
), daily_stats as (
    select
        c.blockchain
        , c.last_1_days_tx_count
        , p.previous_1_days_tx_count
        , (cast(c.last_1_days_tx_count as double) - coalesce(cast(p.previous_1_days_tx_count as double), 0)) / coalesce(cast(p.previous_1_days_tx_count as double), 1) AS daily_percent_change
        , t.total_cross_chain_last_1_days_tx_count
        , cast(c.last_1_days_tx_count as double) / cast(t.total_cross_chain_last_1_days_tx_count as double) AS percent_total_last_1_days_tx_count
    from
        current_day as c
    left join previous_day as p
        on c.blockchain = p.blockchain
    inner join total_current_day_txs as t
        on 1 = 1
), current_week as (
    select
        blockchain
        , sum(tx_count) as last_7_days_tx_count
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
        , sum(tx_count) as previous_7_days_tx_count
    from
        source
    where
        block_date >= date_trunc('day', now()) - interval '14' day
        and block_date < date_trunc('day', now()) - interval '7' day
    group by
        blockchain
), total_current_week_txs as (
    select
        sum(last_7_days_tx_count) AS total_cross_chain_last_7_days_tx_count
    from
        current_week
), weekly_stats as (
    select
        c.blockchain
        , c.last_7_days_tx_count
        , p.previous_7_days_tx_count
        , (cast(c.last_7_days_tx_count as double) - coalesce(cast(p.previous_7_days_tx_count as double), 0)) / coalesce(cast(p.previous_7_days_tx_count as double), 1) AS weekly_percent_change
        , t.total_cross_chain_last_7_days_tx_count
        , cast(c.last_7_days_tx_count as double) / cast(t.total_cross_chain_last_7_days_tx_count as double) AS percent_total_last_7_days_tx_count
    from
        current_week as c
    left join previous_week as p
        on c.blockchain = p.blockchain
    inner join total_current_week_txs as t
        on 1 = 1
), current_month as (
    select
        blockchain
        , sum(tx_count) as last_30_days_tx_count
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
        , sum(tx_count) as previous_30_days_tx_count
    from
        source
    where
        block_date >= date_trunc('day', now()) - interval '60' day
        and block_date < date_trunc('day', now()) - interval '30' day
    group by
        blockchain
), total_current_month_txs as (
    select
        sum(last_30_days_tx_count) AS total_cross_chain_last_30_days_tx_count
    from
        current_month
), monthly_stats as (
    select
        c.blockchain
        , c.last_30_days_tx_count
        , p.previous_30_days_tx_count
        , (cast(c.last_30_days_tx_count as double) - coalesce(cast(p.previous_30_days_tx_count as double), 0)) / coalesce(cast(p.previous_30_days_tx_count as double), 1) AS monthly_percent_change
        , t.total_cross_chain_last_30_days_tx_count
        , cast(c.last_30_days_tx_count as double) / cast(t.total_cross_chain_last_30_days_tx_count as double) AS percent_total_last_30_days_tx_count
    from
        current_month as c
    left join previous_month as p
        on c.blockchain = p.blockchain
    inner join total_current_month_txs as t
        on 1 = 1
)
select
    d.blockchain
    , d.last_1_days_tx_count
    , d.previous_1_days_tx_count
    , d.daily_percent_change
    , d.total_cross_chain_last_1_days_tx_count
    , d.percent_total_last_1_days_tx_count
    , w.last_7_days_tx_count
    , w.previous_7_days_tx_count
    , w.weekly_percent_change
    , w.total_cross_chain_last_7_days_tx_count
    , w.percent_total_last_7_days_tx_count
    , m.last_30_days_tx_count
    , m.previous_30_days_tx_count
    , m.monthly_percent_change
    , m.total_cross_chain_last_30_days_tx_count
    , m.percent_total_last_30_days_tx_count
    , m.last_30_days_tx_count * 12 as tx_count_run_rate
from
    daily_stats as d
inner join weekly_stats as w
    on d.blockchain = w.blockchain
inner join monthly_stats as m
    on d.blockchain = m.blockchain