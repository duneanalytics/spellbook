{{
    config(
        materialized='incremental',
        alias='eth_safes_balances',
        partition_by=['day'],
        unique_key=['day', 'address', 'balance', 'token_address', 'token_id'],
        on_schema_change='fail',
        file_format='delta',
        incremental_strategy='merge',
        post_hook="{{ expose_spells(
           '[\"ethereum\"]', 
           'project', 
           'safe', 
           '[\"safeintern\"]') }}"
    )
}}

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
    from {{ ref('tokens_ethereum_balances_daily_agg') }} a
    join (
        select
            address, blockchain
        from {{ ref('safe_ethereum_safes') }} s
        where blockchain = 'ethereum'
    ) q on q.address = a.address
    where day >= date('2021-07-01') and token_standard in ('native', 'erc20')
    {% if is_incremental() %}
        and day > (select max(day) from {{ this }})
    {% endif %}
),
days as (
    select *
    from unnest(
        sequence(cast('2021-07-01' as date), date(date_trunc('day', now())), interval '1' day)
    ) as foo(day)
),
forward_fill as (
    select
        blockchain,
        cast(d.day as timestamp) as day,
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
    where d.day >= cast('2021-07-01' as date)
)
select
    b.day,
    b.blockchain,
    sum(b.balance * p.price) as balance_usd
from (
    select * from forward_fill
    where balance > 0
) b
left join {{ ref('prices_usd_daily') }} p
    on (token_standard = 'erc20'
    and b.blockchain = p.blockchain
    and b.token_address = p.contract_address
    and b.day = p.day)
    or (token_standard = 'native'
    and p.blockchain is null
    and p.contract_address is null
    and p.symbol = 'ETH'
    and b.day = p.day)
group by 1, 2
