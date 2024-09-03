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

with changed_balances as (
    select
        a.blockchain,
        day,
        a.address,
        token_symbol,
        token_address,
        token_standard,
        token_id,
        balance,
        lead(cast(day as timestamp)) over (partition by token_address, a.address, token_id order by day asc) as next_update_day
    from {{ source('tokens_polygon', 'balances_daily_agg') }} a
    join (
        select
            address
            , blockchain
        from {{ ref('safe_polygon_safes') }} s
        where blockchain = 'polygon'
    ) q on q.address = a.address
    where day >= date '{{ project_start_date }}'
        and token_standard in ('native', 'erc20')
        {% if is_incremental() %}
        and {{ incremental_predicate('day') }}
        {% endif %}
),
days as (
    select *
    from unnest(
        sequence(cast('{{ project_start_date }}' as date), date(date_trunc('day', now())), interval '1' day)
    ) as foo(day)
),
forward_fill as (
    select
        blockchain,
        cast(d.day as date) as day,
        address,
        token_symbol,
        token_address,
        token_standard,
        token_id,
        balance
    from days d
    left join changed_balances b
        on d.day >= b.day
        and (b.next_update_day is null OR d.day < b.next_update_day) 
    where d.day >= date '{{ project_start_date }}'
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
    select * from forward_fill
    where balance > 0
) b
left join {{ ref('prices_usd_daily') }} p
    on (
        token_standard = 'erc20'
        and b.blockchain = p.blockchain
        and b.token_address = p.contract_address
        and b.day = p.day
    )
    or (
        token_standard = 'native'
        and p.blockchain is null
        and p.contract_address is null
        and p.symbol = 'ETH'
        and b.day = p.day
    )
group by 1, 2, 3, 4, 5, 6, 7
