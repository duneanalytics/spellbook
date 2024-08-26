{%- macro balances_daily(balances_daily_agg, start_date, native_token='ETH') %}

with changed_balances as (
    select
    blockchain
    ,day
    ,address
    ,token_symbol
    ,token_address
    ,token_standard
    ,token_id
    ,balance
    ,lead(cast(day as timestamp)) over (partition by token_address,address,token_id order by day asc) as next_update_day
    from {{balances_daily_agg}}
)

,days as (
    select *
    from unnest(
         sequence(cast('{{start_date}}' as date)
                , date(date_trunc('day',now()))
                , interval '1' day
                )
         ) as foo(day)
)

, forward_fill as (
    select
        blockchain
        ,cast(d.day as timestamp) as day
        ,address
        ,token_symbol
        ,token_address
        ,token_standard
        ,token_id
        ,balance
        from days d
        left join changed_balances b
            ON  d.day >= b.day
            and (b.next_update_day is null OR d.day < b.next_update_day) -- perform forward fill
)

select
    b.*
    ,b.balance * p.price as balance_usd
from(
    select * from forward_fill
    where balance > 0
    ) b
left join {{source('prices','usd')}} p
    on (token_standard = 'erc20'
    and b.blockchain = p.blockchain
    and b.token_address = p.contract_address
    and b.day = p.minute)
    or (token_standard = 'native'
    and p.blockchain is null
    and p.contract_address is null
    and p.symbol = '{{native_token}}'
    and b.day = p.minute)

{% endmacro %}
