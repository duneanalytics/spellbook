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
    ,balance_raw
    ,lead(cast(day as timestamp)) over (partition by token_address,address,token_id order by day asc) as next_update_day
    from {{balances_daily_agg}}
    where balance_raw > 0
    {% if var('address_param', 'all') != 'all' %}
    AND address = LOWER('{{ var('address_param') }}')
    {% endif %}
    {% if var('token_address_param', 'all') != 'all' %}
    AND token_address = LOWER('{{ var('token_address_param') }}')
    {% endif %}
    {% if var('blockchain_param', 'all') != 'all' %}
    AND blockchain = LOWER('{{ var('blockchain_param') }}')
    {% endif %}
)

,days_range as (
    select
        min(day) as min_day,
        coalesce(max(next_update_day) - interval '1' day, date(date_trunc('day',now()))) as max_day
    from changed_balances
)

,days as (
    select day_val as day
    from days_range dr
    CROSS JOIN unnest(
         sequence( dr.min_day
                 , dr.max_day
                 , interval '1' day
                )
         ) as foo(day_val)
    WHERE dr.min_day IS NOT NULL AND dr.max_day IS NOT NULL AND dr.min_day <= dr.max_day
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
