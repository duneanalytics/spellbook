{{config(alias='balances')}}

with pools as (
    select pool as pools
    from {{ source('balancer_ethereum', 'BFactory_evt_LOG_NEW_POOL') }}
),

joins as (
    select
        p.pools as pool,
        date_trunc('day', e.evt_block_time) as day,
        e.contract_address as token,
        sum(value) as amount
    from {{ source('erc20_ethereum', 'evt_transfer') }} e
    inner join pools p on e.`to` = p.pools
    group by 1, 2, 3

    union all

    select
        e.`to` as pool,
        date_trunc('day', e.evt_block_time) as day,
        e.contract_address as token,
        sum(value) as amount
    from {{ source('erc20_ethereum', 'evt_transfer') }} e
    where e.`to` = '0xba12222222228d8ba445958a75a0704d566bf2c8'
    group by 1, 2, 3
),

exits as (
    select
        p.pools as pool,
        date_trunc('day', e.evt_block_time) as day,
        e.contract_address as token,
        -sum(value) as amount
    from {{ source('erc20_ethereum', 'evt_transfer') }} e
    inner join pools p on e.`from` = p.pools
    group by 1, 2, 3

    union all

    select
        e.`from` as pool,
        date_trunc('day', e.evt_block_time) as day,
        e.contract_address as token, -sum(value) as amount
    from {{ source('erc20_ethereum', 'evt_transfer') }} e
    where e.`from` = '0xba12222222228d8ba445958a75a0704d566bf2c8'
    group by 1, 2, 3
),

daily_delta_balance_by_token as (
    select
        pool,
        day,
        token,
        sum(coalesce(amount, 0)) as amount from
        (   select *
            from joins j
            union all
            select *
            from exits e
        ) foo
        group by 1, 2, 3
),

cumulative_balance_by_token as (
    select
        pool,
        token,
        `day`,
        lead(day, 1, now()) over (
            partition by token, pool
            order by `day`
        ) as day_of_next_change,
        sum(amount) over (
            partition by pool, token
            order by `day`
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as cumulative_amount
    from daily_delta_balance_by_token
),

calendar as (
    select explode(sequence(to_date('2022-01-01'), current_date, interval 1 day))
),

running_cumulative_balance_by_token_deprecated as (
    select
        c.day,
        pool,
        token,
        cumulative_amount
    from calendar c
    left join cumulative_balance_by_token b on b.day <= c.day and c.day < b.day_of_next_change
),

running_cumulative_balance_by_token as (
    select
        dateadd(b.day, i) as `day`,
        pool,
        token,
        cumulative_amount
    from cumulative_balance_by_token b
        lateral view outer
        posexplode(split(space(datediff(day_of_next_change, b.day)), ' ')) temp as i,x
    where to_date('2022-01-01') <= b.day and b.bay <= current_date
)

select
    `day`,
    pool,
    token,
    coalesce(cumulative_amount, 0) as cumulative_amount
from running_cumulative_balance_by_token