{{ config(
        schema = 'fees'
        , alias = 'daily_blockchain_rank'
        )
}}

with previous_period as (
    select
        blockchain
        , day as previous_day
        , gas_spent_usd as previous_day_gas_spent_usd
    from
        {{ ref('fees_daily') }}
    where
        day >= date_trunc('day', now()) - interval '2' day
        and day < date_trunc('day', now()) - interval '1' day
), current_period as (
    select
        blockchain
        , day as current_day
        , gas_spent_usd as current_day_gas_spent_usd
    from
        {{ ref('fees_daily') }}
    where
        day >= date_trunc('day', now()) - interval '1' day
        and day < date_trunc('day', now())
), total_current_fees as (
    select
        sum(current_day_gas_spent_usd) AS total_current_fees
    from
        current_period
)
select
    cp.blockchain
    , cp.current_day_gas_spent_usd
    , pp.previous_day_gas_spent_usd
    , ((cp.current_day_gas_spent_usd - coalesce(pp.previous_day_gas_spent_usd, 0)) / coalesce(pp.previous_day_gas_spent_usd, 1)) * 100 AS percent_change
    , (cp.current_day_gas_spent_usd / t.total_current_fees) * 100 AS percent_of_total_current_fees
from
    current_period as cp
left join previous_period as pp
    on cp.blockchain = pp.blockchain
inner join total_current_fees as t
    on 1 = 1