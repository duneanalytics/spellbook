{{
    config(
        schema = 'safe_polygon',
        alias = 'balances',
        partition_by = ['day'],
        materialized = 'incremental',
        incremental_strategy = 'merge',
        file_format = 'delta',
        unique_key = ['day', 'blockchain', 'address', 'token_address'],
        post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                      spell_type = "project",
                                      spell_name = "safe",
                                      contributors = \'["safeintern"]\') }}'
    )
}}

{% set project_start_date = '2021-07-01' %}

with safes as (
    select
        address,
        blockchain
    from {{ ref('safe_polygon_safes') }}
    where blockchain = 'polygon'
),
token_balances as (

    select
        a.blockchain,
        a.day,
        a.address,
        a.token_symbol,
        a.token_address,
        a.token_standard,
        a.token_id,
        a.balance,
        lead(cast(day as timestamp)) over (partition by token_address, a.address, token_id order by day asc) as next_update_day
    from {{ source('tokens_polygon', 'balances_daily_agg') }} a
    join safes s on a.address = s.address
    where a.day >= date('2021-07-01')
        and a.token_standard in ('native', 'erc20')
        {% if is_incremental() %}
        and {{ incremental_predicate('a.day') }}
        {% endif %}
),
days as (
    -- Generate a sequence of days to ensure forward-filling
    select *
    from unnest(
        sequence(cast('2021-07-01' as date), date(date_trunc('day', now())), interval '1' day)
    ) as foo(day)
),
forward_fill as (
    -- Forward-fill balances across all safes, even if there's no balance change
    select
        s.blockchain,
        cast(d.day as date) as day,
        s.address,
        coalesce(b.token_symbol, lag(b.token_symbol) over (partition by s.address, b.token_address order by d.day)) as token_symbol,
        coalesce(b.token_address, lag(b.token_address) over (partition by s.address, b.token_address order by d.day)) as token_address,
        coalesce(b.token_standard, lag(b.token_standard) over (partition by s.address, b.token_address order by d.day)) as token_standard,
        coalesce(b.token_id, lag(b.token_id) over (partition by s.address, b.token_address order by d.day)) as token_id,
        coalesce(b.balance, lag(b.balance) over (partition by s.address, b.token_address order by d.day)) as balance
    from days d
    cross join safes s
    left join token_balances b
        on s.address = b.address
        and d.day >= b.day
        and (b.next_update_day is null OR d.day < b.next_update_day)
    where d.day >= cast('2021-07-01' as date)
        {% if is_incremental() %}
        and {{ incremental_predicate('d.day') }}
        {% endif %}
)
select
    b.day,
    b.blockchain,
    b.address,
    b.token_address,
    b.token_standard,
    b.token_id,
    b.token_symbol,
    sum(b.balance) as token_balance,
    sum(b.balance * p.price) as balance_usd
from (
    -- Ensure that we only include records with positive balances for final output
    select * from forward_fill
    where balance > 0
) b
left join {{ ref('prices_usd_daily') }} p
    on (
        b.token_standard = 'erc20'
        and b.blockchain = p.blockchain
        and b.token_address = p.contract_address
        and b.day = p.day
    )
    or (
        b.token_standard = 'native'
        and p.blockchain is null
        and p.contract_address is null
        and p.symbol = 'POL'
        and b.day = p.day
    )
group by 1, 2, 3, 4, 5, 6, 7
