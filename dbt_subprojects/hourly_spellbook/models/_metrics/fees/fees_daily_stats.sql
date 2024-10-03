{{ config(
        schema = 'fees'
        , alias = 'daily_stats'
        )
}}

with previous_period as (
    select
        day as previous_day
        , sum(gas_spent_usd) as previous_day_gas_spent_usd
    from
        {{ ref('fees_daily') }}
    where
        day >= date_trunc('day', now()) - interval '2' day
        and day < date_trunc('day', now()) - interval '1' day
    group by
        day
), current_period as (
    select
        day
        , sum(gas_spent_usd) as current_day_gas_spent_usd
    from
        {{ ref('fees_daily') }}
    where
        day >= date_trunc('day', now()) - interval '1' day
        and day < date_trunc('day', now())
    group by
        day
)
select
    cp.day
    , cp.current_day_gas_spent_usd
    , pp.previous_day_gas_spent_usd
    , (cp.current_day_gas_spent_usd - coalesce(pp.previous_day_gas_spent_usd, 0)) / coalesce(pp.previous_day_gas_spent_usd, 1) * 100 AS percent_change
    , (cp.current_day_gas_spent_usd * 365) as gas_spent_usd_annual_run_rate
from
    current_period as cp
inner join previous_period as pp
    on 1 = 1