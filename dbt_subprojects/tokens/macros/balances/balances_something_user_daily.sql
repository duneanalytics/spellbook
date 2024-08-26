{#  @DEV here

    @NOTICE this macro constructs the user level token table for given input table
    @NOTICE aka, you give a list of tokens, it generates table with daily balances of each user holding that token (useful for holder analysis)

    @PARAM balances_raw             -- raw_balances source
    @PARAM something                -- must have the following columns [category,project,version,token_address]
    
#}

{%- macro balances_daily(
        balances_daily_agg, 
        something,
        start_date, 
        native_token='ETH'
    ) 
%}

{% set incremental_logic = """
    and date_trunc('day',evt_block_time) > date(now()) - interval '""" + aave_incremental_overlap_window() + """ days'
"""%}

with 
tokens as (
    select 
        category,
        project,
        version,
        token_address
    from {ref(something)}
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
        and token_address in (select token_address from tokens)
    {% if is_incremental() %}
        {{incremental_logic}}
    {% endif %}
    -- {% if is_incremental() %}
    -- WHERE
    --     {{ incremental_predicate('t.evt_block_time') }}
    -- {% endif %}
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

{% endmacro %}
