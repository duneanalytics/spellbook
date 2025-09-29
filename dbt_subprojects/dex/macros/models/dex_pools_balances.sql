{% macro dex_pools_balances(
    blockchain = null
    , start_date  = null
    , pools_table = null
    , pools_column = 'id'
    )
%}

with 

pool_addresses as (
    select 
        {{pools_column}} as address 
    from 
    {{ pools_table }}
),

filtered_balances AS (
  {{ balances_incremental_subset_daily(
       blockchain=blockchain,
       start_date=start_date,
       address_list='pool_addresses'
  ) }}
)

select 
    * 
from 
filtered_balances
where token_standard in ('erc20', 'native')

{% endmacro %}


{% macro enrich_dex_pools_balances(
    dex_pools_balances = null
    , pools_table = null 
    , token0  = null
    , token1 = null 
    , token2 = null 
    , token3 = null 
    , pools_column = null 
    , blockchain = null 
    , project = null 
    )
%}

with 

get_balances as (
    select 
        , blockchain
        , day
        , address
        , token_symbol
        , token_address
        , balance 
    from 
    {{ dex_pools_balances }}
    {% if is_incremental() %}
    where {{ incremental_predicate('day') }}
    {% endif %}
),

-- get prices first 
get_prices as (
    select 
        *
        , balance * 1 as balance_usd 
    from 
    get_balances
),

distinct_days as (
    select 
        distinct 
        address
        , day
        , blockchain 
    from 
    get_balances
)

    select 
        dd.day
        , '{{ blockchain }}' as blockchain
        , '{{ project }}' as project
        , pt.version
        , pt.{{pools_column}} as id
        {% if token0 %} -- token0
        , t0.token_address as token0 
        , t0.token_symbol as token0_symbol 
        , t0.balance as token0_balance
        , t0.balance_usd as token0_balance_usd 
        {% endif %}
        {% if token1 %} -- token1
        , t1.token_address as token1
        , t1.token_symbol as token1_symbol 
        , t1.balance as token1_balance
        , t1.balance_usd as token1_balance_usd 
        {% endif %}
        {% if token2 %} -- token2
        , t2.token_address as token2 
        , t2.token_symbol as token2_symbol 
        , t2.balance as token2_balance
        , t2.balance_usd as token2_balance_usd 
        {% endif %}
        {% if token3 %} -- token1
        , t3.token_address as token3
        , t3.token_symbol as token3_symbol 
        , t3.balance as token3_balance
        , t3.balance_usd as token3_balance_usd 
        {% endif %}
    from 
    distinct_days dd 
    inner join 
    {{ pools_table }} pt 
        on dd.address = pt.{{pools_column}}
        and dd.blockchain = '{{ blockchain }}'

    {% if token0 %} -- token0
    left join 
    get_prices t0 
        on dd.day = t0.day 
        and dd.address = t0.address 
        and dd.blockchain = t0.blockchcian 
        and pt.{{token0}} = t0.token_address 
    {% endif %}

    {% if token1 %} -- token1
    left join 
    get_prices t1
        on dd.day = t1.day 
        and dd.address = t1.address 
        and dd.blockchain = t1.blockchcian 
        and pt.{{token1}} = t1.token_address 
    {% endif %}

    {% if token2 %} -- token2
    left join 
    get_prices t2
        on dd.day = t2.day 
        and dd.address = t2.address 
        and dd.blockchain = t2.blockchcian 
        and pt.{{token2}} = t2.token_address 
    {% endif %}

    {% if token3 %} -- token3
    left join 
    get_prices t3
        on dd.day = t3.day 
        and dd.address = t3.address 
        and dd.blockchain = t3.blockchcian 
        and pt.{{token3}} = t3.token_address 
    {% endif %}

{% endmacro %}