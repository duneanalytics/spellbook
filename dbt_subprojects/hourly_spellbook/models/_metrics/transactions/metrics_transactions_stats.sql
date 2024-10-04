{{ config(
        schema = 'metrics'
        , alias = 'transactions_stats'
        )
}}

with source as (
    -- daily aggregation of tx's per blockchain
    select
        blockchain
        , day
        , tx_count
    from
        {{ ref('metrics_transactions_daily') }}
), current_day as (
    select
        blockchain
        , tx_count as current_day_tx_count
    from
        source
    where
        day >= date_trunc('day', now()) - interval '1' day
        and day < date_trunc('day', now())
), previous_day as (
    select
        blockchain
        , tx_count as previous_day_tx_count
    from
        source
    where
        day >= date_trunc('day', now()) - interval '2' day
        and day < date_trunc('day', now()) - interval '1' day
), total_current_day_txs as (
    select
        sum(current_day_tx_count) AS total_current_day_txs
    from
        current_day
), daily_stats as (
    select
        c.blockchain
        , c.current_day_tx_count
        , p.previous_day_tx_count
        , ((c.current_day_tx_count - coalesce(p.previous_day_tx_count, 0)) / coalesce(p.previous_day_tx_count, 1)) * 100 AS daily_percent_change
        , t.total_current_day_txs
        , (c.current_day_tx_count / t.total_current_day_txs) * 100 AS percent_of_total_current_day_txs
    from
        current_day as c
    left join previous_day as p
        on c.blockchain = p.blockchain
    inner join total_current_day_txs as t
        on 1 = 1
), current_week as (
    select
        blockchain
        , sum(tx_count) as current_week_tx_count
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
        , sum(tx_count) as previous_week_tx_count
    from
        source
    where
        day >= date_trunc('day', now()) - interval '14' day
        and day < date_trunc('day', now()) - interval '7' day
    group by
        blockchain
), total_current_week_txs as (
    select
        sum(current_week_tx_count) AS total_current_week_txs
    from
        current_week
), weekly_stats as (
    select
        c.blockchain
        , c.current_week_tx_count
        , p.previous_week_tx_count
        , ((c.current_week_tx_count - coalesce(p.previous_week_tx_count, 0)) / coalesce(p.previous_week_tx_count, 1)) * 100 AS weekly_percent_change
        , t.total_current_week_txs
        , (c.current_week_tx_count / t.total_current_week_txs) * 100 AS percent_of_total_current_week_txs
    from
        current_week as c
    left join previous_week as p
        on c.blockchain = p.blockchain
    inner join total_current_week_txs as t
        on 1 = 1
), current_month as (
    select
        blockchain
        , sum(tx_count) as current_month_tx_count
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
        , sum(tx_count) as previous_month_tx_count
    from
        source
    where
        day >= date_trunc('day', now()) - interval '60' day
        and day < date_trunc('day', now()) - interval '30' day
    group by
        blockchain
), total_current_month_txs as (
    select
        sum(current_month_tx_count) AS total_current_month_txs
    from
        current_month
), monthly_stats as (
    select
        c.blockchain
        , c.current_month_tx_count
        , p.previous_month_tx_count
        , ((c.current_month_tx_count - coalesce(p.previous_month_tx_count, 0)) / coalesce(p.previous_month_tx_count, 1)) * 100 AS monthly_percent_change
        , t.total_current_month_txs
        , (c.current_month_tx_count / t.total_current_month_txs) * 100 AS percent_of_total_current_month_txs
    from
        current_month as c
    left join previous_month as p
        on c.blockchain = p.blockchain
    inner join total_current_month_txs as t
        on 1 = 1
)
select
    d.blockchain
    , d.current_day_tx_count
    , d.previous_day_tx_count
    , d.daily_percent_change
    , d.total_current_day_txs
    , d.percent_of_total_current_day_txs
    , w.current_week_tx_count
    , w.previous_week_tx_count
    , w.weekly_percent_change
    , w.total_current_week_txs
    , w.percent_of_total_current_week_txs
    , m.current_month_tx_count
    , m.previous_month_tx_count
    , m.monthly_percent_change
    , m.total_current_month_txs
    , m.percent_of_total_current_month_txs
    , m.total_current_month_txs * 12 as tx_count_run_rate
from
    daily_stats as d
inner join weekly_stats as w
    on d.blockchain = w.blockchain
inner join monthly_stats as m
    on d.blockchain = m.blockchain