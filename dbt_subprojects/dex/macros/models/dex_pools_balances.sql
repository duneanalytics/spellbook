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
    , token0_weth = null 
    , token1_weth = null 
    , pools_column = null 
    , blockchain = null 
    , project = null 
    , native_token_symbol = null 
    , pool_native_token_address = null 
    , balances_native_token_address = null
    , weth_address = null 
    )
%}

with 

get_balances as (
    select 
        blockchain
        , day
        , address
        , if (token_standard = 'native', '{{native_token_symbol}}', token_symbol) as token_symbol
        , token_address
        , balance 
    from 
    {{ dex_pools_balances }}
    {% if is_incremental() %}
    where {{ incremental_predicate('day') }}
    {% endif %}
),

get_pools as (
    select 
        pt.version
        , pt.{{pools_column}} as id
        {% if token0 %} 
        , if (
                pt.{{token0}} = {{pool_native_token_address}}, 
                {{balances_native_token_address}}, 
                pt.{{token0}}
            ) as token0
        {% endif %}
        {% if token1 %} 
        , if (
                pt.{{token1}} = {{pool_native_token_address}}, 
                {{balances_native_token_address}}, 
                pt.{{token1}}
            ) as token1
        {% endif %}
        {% if token2 %} 
        , pt.{{token2}} as token2
        {% endif %}
        {% if token3 %} 
        , pt.{{token3}} as token3
        {% endif %}
    from 
    {{ pools_table }} pt 
),

prices as (
    select
        cast(date_trunc('day', minute) as date) as block_date
        , blockchain
        , contract_address
        , max_by(price, minute) as price
    from 
    {{ source('prices','usd_with_native') }}
    where 1 = 1 
    {% if is_incremental() %}
    and {{ incremental_predicate('minute') }}
    {% endif %}
    and not (blockchain = 'ethereum' and contract_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee)
    group by 1, 2, 3 

    union all 

    select
        cast(date_trunc('day', minute) as date) as block_date
        , blockchain
        , 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as contract_address
        , max_by(price, minute) as price
    from 
    {{ source('prices','usd_with_native') }}
    where 1 = 1 
    {% if is_incremental() %}
    and {{ incremental_predicate('minute') }}
    {% endif %}
    and blockchain = 'ethereum'
    and contract_address = 0x0000000000000000000000000000000000000000
    group by 1, 2, 3 
    -- ethereum's native contract address in prices is 0x..000 while the native contract in balances is 0x...eee
),

prices_day as (
    select
        cast(date_trunc('day', timestamp) as date) as block_date
        , blockchain
        , contract_address
        , price
    from 
    {{ source('prices','day') }}
    where volume is not null 
    and volume > 500000 -- greater than $500k day volume 
    {% if is_incremental() %}
    and {{ incremental_predicate('timestamp') }}
    {% endif %}
),

get_prices as (
    select 
        gb.*
        , gb.balance * coalesce(p.price, pd.price) as balance_usd 
    from 
    get_balances gb 
    left join 
    prices p 
        on gb.token_address = p.contract_address 
        and gb.blockchain = p.blockchain 
        and gb.day = p.block_date 
    left join 
    prices_day pd
        on gb.token_address = pd.contract_address 
        and gb.blockchain = pd.blockchain 
        and gb.day = pd.block_date 
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
        date_trunc('month', dd.day) as block_month
        , dd.day as block_date
        , '{{ blockchain }}' as blockchain
        , '{{ project }}' as project
        , pt.version
        , pt.id
        {% if token0 %} -- token0
        {% if token0_weth %} -- this additional logic only affects curve pools since some pools have the WETH address as the token address but they're actually holding ETH in the wallet and not WETH so if they have no WETH balance, we should replace with ETH balance
        , case 
            when pt.token0 = {{weth_address}}
            and (t0.balance = 0 or t0.balance is null)
            then coalesce(t0w.token_address, pt.token0) -- if null return original value
            else pt.token0
        end as token0
        , case 
            when pt.token0 = {{weth_address}}
            and (t0.balance = 0 or t0.balance is null)
            then coalesce(t0w.token_symbol, t0.token_symbol)
            else t0.token_symbol
        end as token0_symbol 
        , case 
            when pt.token0 = {{weth_address}}
            and (t0.balance = 0 or t0.balance is null)
            then coalesce(t0w.balance, t0.balance)
            else t0.balance
        end as token0_balance
        , case 
            when pt.token0 = {{weth_address}}
            and (t0.balance = 0 or t0.balance is null)
            then coalesce(t0w.balance_usd, t0.balance_usd)
            else t0.balance_usd
        end as token0_balance_usd
        {% else %}
        , t0.token_address as token0 
        , t0.token_symbol as token0_symbol 
        , t0.balance as token0_balance
        , t0.balance_usd as token0_balance_usd
        {% endif %}
        {% endif %}
        {% if token1 %} -- token1
        {% if token1_weth %} -- this additional logic only affects curve pools since some pools have the WETH address as the token address but they're actually holding ETH in the wallet and not WETH so if they have no WETH balance, we should replace with ETH balance
        , case 
            when pt.token1 = {{weth_address}}
            and (t1.balance = 0 or t1.balance is null)
            then coalesce(t1w.token_address, pt.token1) -- if null return original value
            else pt.token1
        end as token1
        , case 
            when pt.token1 = {{weth_address}}
            and (t1.balance = 0 or t1.balance is null)
            then coalesce(t1w.token_symbol, t1.token_symbol)
            else t1.token_symbol
        end as token1_symbol 
        , case 
            when pt.token1 = {{weth_address}}
            and (t1.balance = 0 or t1.balance is null)
            then coalesce(t1w.balance, t1.balance)
            else t1.balance
        end as token1_balance
        , case 
            when pt.token1 = {{weth_address}}
            and (t1.balance = 0 or t1.balance is null)
            then coalesce(t1w.balance_usd, t1.balance_usd)
            else t1.balance_usd
        end as token1_balance_usd
        {% else %}
        , t1.token_address as token1
        , t1.token_symbol as token1_symbol 
        , t1.balance as token1_balance
        , t1.balance_usd as token1_balance_usd
        {% endif %}
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
    get_pools pt 
        on dd.address = pt.id
        and dd.blockchain = '{{ blockchain }}'

    {% if token0 %} -- token0
    left join 
    get_prices t0 
        on dd.day = t0.day 
        and dd.address = t0.address 
        and dd.blockchain = t0.blockchain
        and pt.token0 = t0.token_address 
    {% if token0_weth %}
    left join 
    get_prices t0w
        on dd.day = t0w.day 
        and dd.address = t0w.address 
        and dd.blockchain = t0w.blockchain
        and pt.token0 = {{weth_address}}
        and t0w.token_address = {{token0_weth}}
    {% endif %}
    {% endif %}

    {% if token1 %} -- token1
    left join 
    get_prices t1
        on dd.day = t1.day 
        and dd.address = t1.address 
        and dd.blockchain = t1.blockchain
        and pt.token1 = t1.token_address 
    {% if token1_weth %}
    left join 
    get_prices t1w
        on dd.day = t1w.day 
        and dd.address = t1w.address 
        and dd.blockchain = t1w.blockchain
        and pt.token1 = {{weth_address}}
        and t1w.token_address = {{token1_weth}}
    {% endif %}
    {% endif %}

    {% if token2 %} -- token2
    left join 
    get_prices t2
        on dd.day = t2.day 
        and dd.address = t2.address 
        and dd.blockchain = t2.blockchain 
        and pt.token2 = t2.token_address 
    {% endif %}

    {% if token3 %} -- token3
    left join 
    get_prices t3
        on dd.day = t3.day 
        and dd.address = t3.address 
        and dd.blockchain = t3.blockchain
        and pt.token3 = t3.token_address 
    {% endif %}

{% endmacro %}
