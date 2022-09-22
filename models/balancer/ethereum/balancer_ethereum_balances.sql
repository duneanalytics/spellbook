{{ config(
    alias = 'balances',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                            "sector",
                                            "balancer",
                                            \'["justabi"]\') }}'
) }}

with pools as (
    select pool
    from {{ source('balancer_ethereum', 'bfactory_evt_log_new_pool') }}
),

evt_transfer as (
    select *
    from {{ source('erc20_ethereum', 'evt_transfer') }}
),

joins as (
    select pools.pool,
           date_trunc('day', evt_transfer.evt_block_time) as day,
           evt_transfer.contract_address as token,
           sum(value) as amount
    from evt_transfer
    inner join pools on evt_transfer.`to` = pools.pool
    {{ dbt_utils.group_by(3) }}

    union all

    select `to` as pool,
           date_trunc('day', evt_transfer.evt_block_time) as day,
           contract_address as token,
           sum(value) as amount
    from evt_transfer
    where `to` = '0xba12222222228d8ba445958a75a0704d566bf2c8'
    {{ dbt_utils.group_by(3) }}
),

exits as (
    select pools.pool,
           date_trunc('day', evt_transfer.evt_block_time) as day,
           evt_transfer.contract_address as token,
           -sum(value) as amount
    from evt_transfer
    inner join pools on evt_transfer.`from` = pools.pool
    {{ dbt_utils.group_by(3) }}

    union all

    select `from` as pool,
           date_trunc('day', evt_transfer.evt_block_time) as day,
           contract_address as token,
           -sum(value) as amount
    from evt_transfer
    where `from` = '0xba12222222228d8ba445958a75a0704d566bf2c8'
    {{ dbt_utils.group_by(3) }}
),

joins_union_exits as (
    select *
    from joins

    union all

    select *
    from exits
),

daily_delta_balance_by_token as (
    select pool,
           day,
           token,
           sum(coalesce(amount, 0)) as amount
    from joins_union_exits
    {{ dbt_utils.group_by(3) }}
),

cumulative_balance_by_token as (
    select pool,
           token,
           day,
           lead(day, 1, now()) over (partition by token, pool order by day) as day_of_next_change,
           sum(amount) over (partition by pool, token order by day rows between unbounded preceding and current row) as cumulative_amount
    from daily_delta_balance_by_token
),

calendar as (
    select explode(sequence(to_date('2022-01-01'), current_date, interval 1 day)) as day
),

running_cumulative_balance_by_token as (
    select calendar.day,
           pool,
           token,
           cumulative_amount
    from calendar
    inner join cumulative_balance_by_token cumu_bal
        on calendar.day >= cumu_bal.day  and calendar.day < cumu_bal.day_of_next_change
)

select *
from running_cumulative_balance_by_token
