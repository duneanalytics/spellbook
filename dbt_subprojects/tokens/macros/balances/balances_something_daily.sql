{#  @DEV here

    @NOTICE this macro constructs the address level token balances table for given input table
    @NOTICE aka, you give a list of tokens and address, it generates table with daily balances of each address token pair (useful for TVL calculations)

    @PARAM balances_raw             -- raw_balances source
    @PARAM something                -- must have the following columns [category,project,version,address,token_address]
    @PARAM start_date               -- start date from which data needs to be populated
    @PARAM has_address              -- does the tokens list have a address column, to only balances of specific addresses are selected
    
#}

{%- macro balances_something_daily(
        balances_daily_agg, 
        something,
        start_date, 
        has_address = 0,
        native_token='ETH'
    ) 
%}


tokens as (
    select 
        category,
        project,
        version,
        {% if has_address %}
          address,
        {% endif %}
        token_address
    from {{something}}
),
changed_balances as (
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
    where day > cast('{{start_date}}' as date)
        {% if has_address %}
        and address in (select address from tokens)
        {% endif %}
        and token_address in (select token_address from tokens)
    {% if is_incremental() %}
        and {{ incremental_predicate('day') }}
    {% endif %}
),
days as (
    select *
    from unnest(
         sequence(cast('{{start_date}}' as date)
                , date(date_trunc('day',now()))
                , interval '1' day
                )
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
            ON  d.day >= b.day
            and (b.next_update_day is null OR d.day < b.next_update_day) -- perform forward fill
)

select
    b.blockchain,
    t.project,
    t.version,
    b.day,
    b.address,
    b.token_symbol,
    b.token_address,
    b.token_standard,
    b.token_id,
    b.balance,
    b.balance * p.price as balance_usd
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
join tokens t 
    on b.token_address = t.token_address
    {% if has_address %}
    and b.address = t.address
    {% endif %}

{% endmacro %}
