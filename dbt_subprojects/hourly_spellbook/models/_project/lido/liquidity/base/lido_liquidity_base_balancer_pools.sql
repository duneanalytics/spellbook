{{ config(
    schema='lido_liquidity_base',
    alias = 'balancer_pools',
     
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool', 'time'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.time')],
    post_hook='{{ expose_spells(blockchains = \'["base"]\',
                                spell_type = "project",
                                spell_name = "lido_liquidity",
                                contributors = \'["pipistrella"]\') }}'
    )
}}

{% set project_start_date = '2024-10-01' %}

with 

 pools(pool_id,poolAddress) as (   
values
(0x54d86e177cdc664b5f9b17eb5fd6a76fa529e466000200000000000000000199, 0x54d86e177cdc664b5f9b17eb5fd6a76fa529e466)

)


, tokens as (
select distinct token_address from (
SELECT  tokens.token_address
FROM {{ source('balancer_v2_base','weightedpoolfactory_call_create')}} call_create
    CROSS JOIN UNNEST(call_create.tokens) as tokens(token_address)
WHERE call_create.output_0 in (select distinct  poolAddress from pools)
union all
SELECT tokens.token_address
FROM {{ source('balancer_v2_base','composablestablepoolfactory_call_create')}} call_create
    CROSS JOIN UNNEST(call_create.tokens) as tokens(token_address)
WHERE call_create.output_0 in (select distinct  poolAddress from pools)
union all
SELECT tokens.token_address
FROM {{ source('gyroscope_base','gyroeclppoolfactory_call_create')}} call_create
    CROSS JOIN UNNEST(call_create.tokens) as tokens(token_address)
WHERE call_create.output_0 in (select distinct  poolAddress from pools)

))


, tokens_prices_daily AS (
    SELECT distinct
        DATE_TRUNC('day', minute) AS time,
        contract_address as token,
        symbol,
        decimals,
        avg(price) AS price
    FROM {{ source('prices','usd')}} p
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE {{ incremental_predicate('p.minute') }}
    {% endif %}

    and date_trunc('day', minute) < date_trunc('day', now())
    and blockchain = 'base'
    and contract_address in (select distinct token_address from tokens)
    group by 1,2,3,4
    union all
    SELECT distinct
        DATE_TRUNC('day', minute), 
        contract_address as token,
        symbol,
        decimals,
        last_value(price) over (partition by DATE_TRUNC('day', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{ source('prices','usd')}}
    WHERE date_trunc('day', minute) = current_date
    and blockchain = 'base'
    and contract_address in (select distinct token_address from tokens)

)

, wsteth_prices_hourly AS (
    select time, lead(time,1, DATE_TRUNC('hour', now() + interval '1' hour)) over (order by time) as next_time, price
    from (
    SELECT distinct
        DATE_TRUNC('hour', minute) time,
        last_value(price) over (partition by DATE_TRUNC('hour', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{ source('prices','usd')}} p
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE {{ incremental_predicate('p.minute') }}
    {% endif %}

    and blockchain = 'base'
    and contract_address = 0xc1cba3fcea344f92d9239c08c0568f6f2f0ee452
))


, swaps_changes as (
        SELECT
            day,
            pool_id,
            token,
            SUM(COALESCE(delta, 0)) AS delta
        FROM
            (
                SELECT
                    date_trunc('day', evt_block_time) AS day,
                    poolId AS pool_id,
                    tokenIn AS token,
                    cast(amountIn as double) AS delta
                FROM {{source('balancer_v2_base','vault_evt_swap')}}
                {% if not is_incremental() %}
                WHERE DATE_TRUNC('day', evt_block_time) >= DATE '{{ project_start_date }}'
                {% else %}
                WHERE {{ incremental_predicate('evt_block_time') }}
                {% endif %}
                
                and poolId in (select pool_id from pools)
                UNION
                ALL
                SELECT
                    date_trunc('day', evt_block_time) AS day,
                    poolId AS pool_id,
                    tokenOut AS token,
                    -cast(amountOut as double) AS delta
                FROM {{source('balancer_v2_base','vault_evt_swap')}}
                {% if not is_incremental() %}
                WHERE DATE_TRUNC('day', evt_block_time) >= DATE '{{ project_start_date }}'
                {% else %}
                WHERE {{ incremental_predicate('evt_block_time') }}
                {% endif %}
                
                and poolId in (select pool_id from pools)
            ) swaps
        GROUP BY 1, 2, 3
)

, balances_changes AS (
        SELECT
            date_trunc('day', evt_block_time) AS day,
            poolId AS pool_id,
            u.token,
            cast(u.delta as double) - cast(u.protocolFeeAmounts as double) as delta
        FROM {{source('balancer_v2_base','vault_evt_poolbalancechanged')}}
         CROSS JOIN UNNEST(tokens, deltas, protocolFeeAmounts) as u(token, delta, protocolFeeAmounts)
        {% if not is_incremental() %}
        WHERE DATE_TRUNC('day', evt_block_time) >= DATE '{{ project_start_date }}'
        {% else %}
        WHERE {{ incremental_predicate('evt_block_time') }}
        {% endif %}
        
        and poolId in (select pool_id from pools)
        ORDER BY 1, 2, 3
    )

, managed_changes AS (
        SELECT
            date_trunc('day', evt_block_time) AS day,
            poolId AS pool_id,
            token,
            --cashDelta + 
            cast(managedDelta as double) AS delta
        FROM {{source('balancer_v2_base','vault_evt_poolbalancemanaged')}}
        {% if not is_incremental() %}
        WHERE DATE_TRUNC('day', evt_block_time) >= DATE '{{ project_start_date }}'
        {% else %}
        WHERE {{ incremental_predicate('evt_block_time') }}
        {% endif %}
        
        and poolId in (select pool_id from pools)
)

, daily_delta_balance AS (
        SELECT
            day,
            pool_id,
            token,
            SUM(COALESCE(amount, 0)) AS amount
        FROM
            (
                SELECT
                    day,
                    pool_id,
                    token,
                    SUM(COALESCE(delta, 0)) AS amount
                FROM
                    balances_changes
                GROUP BY 1, 2, 3
                UNION ALL
                SELECT
                    day,
                    pool_id,
                    token,
                    delta AS amount
                FROM
                    swaps_changes
                UNION ALL
                SELECT
                    day,
                    pool_id,
                    token,
                    delta AS amount
                FROM
                    managed_changes
            ) balance
        GROUP BY 1, 2, 3
)

, balance AS (
        select  day, LEAD(day, 1, current_date + interval '1' day) OVER (PARTITION BY token, pool_id ORDER BY DAY) AS day_of_next_change, 
                pool_id, token, amount
        from (        
        SELECT
            day,
            pool_id,
            token,
            SUM(amount)  AS amount
        FROM daily_delta_balance
        GROUP BY 1,2,3
        )
)

, usd_balance AS (
        SELECT
            day,
            pool_id,
            b.token,
            p1.symbol AS token_symbol,
            amount as token_balance_raw,
            amount / POWER(10, p1.decimals) AS token_balance,
            COALESCE(p1.price,0) as price,
            0 as row_numb
        FROM balance b 
        LEFT JOIN tokens_prices_daily p1 ON p1.time = b.day AND p1.token = b.token
        WHERE b.token in (0xc1cba3fcea344f92d9239c08c0568f6f2f0ee452)
        union all
        SELECT
            day,
            pool_id,            
            b.token,
            p1.symbol AS token_symbol,
            amount as token_balance_raw,
            amount / POWER(10, p1.decimals) AS token_balance,
            COALESCE(p1.price, 0) AS price, 
            row_number() OVER(PARTITION BY day, pool_id ORDER BY  day, pool_id, b.token) as row_numb
        FROM balance b 
        LEFT JOIN tokens_prices_daily p1 ON p1.time = b.day AND p1.token = b.token
        WHERE b.token not in (select distinct poolAddress from pools
                            union all
                            select 0xc1cba3fcea344f92d9239c08c0568f6f2f0ee452
                            )
        
)




, reserves as (
select  main.day, main.pool_id, 
        main_token, main_token_symbol, main_token_reserve, main_token_usd_price,
        paired1_token, paired1_token_symbol, paired1_token_reserve, paired1_token_usd_price

from (
SELECT
    b.day,
    b.pool_id,
    token AS main_token,
    token_symbol as main_token_symbol,
    coalesce(token_balance, token_balance_raw) as main_token_reserve,
    coalesce(price, 0) AS main_token_usd_price
FROM usd_balance b
where token in (0xc1cba3fcea344f92d9239c08c0568f6f2f0ee452) ) main
join 
(
SELECT
    b.day,
    b.pool_id,
    token AS paired1_token,
    token_symbol as paired1_token_symbol,
    coalesce(token_balance, token_balance_raw) as paired1_token_reserve,
    coalesce(price, 0) AS paired1_token_usd_price
FROM usd_balance b
where token not in  (0xc1cba3fcea344f92d9239c08c0568f6f2f0ee452) and cast(row_numb as int) = int '1') paired1
on main.day = paired1.day and main.pool_id = paired1.pool_id 
)

, trading_volume as (    
    select  date_trunc('day', s.evt_block_time) as time,
        poolId,
        sum(
        case when tokenOut in (0xc1cba3fcea344f92d9239c08c0568f6f2f0ee452) then wsteth_price.price*amountOut/1e18 
             when tokenIn in  (0xc1cba3fcea344f92d9239c08c0568f6f2f0ee452) then  wsteth_price.price*amountIn/1e18 
             else 0 end) as trading_volume
    from {{source('balancer_v2_base','vault_evt_swap')}} s
    left join wsteth_prices_hourly wsteth_price on date_trunc('hour', s.evt_block_time) >= wsteth_price.time 
    and date_trunc('hour', s.evt_block_time) < wsteth_price.next_time
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', s.evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE {{ incremental_predicate('s.evt_block_time') }}
    {% endif %} 
    and s.poolId in (select pool_id from pools)
    group by 1,2
) 

, all_metrics as (
select  pool_id as pool, 'base' as blockchain, 'balancer' as project, 0 as fee, 
        cast(day as date) as time, main_token, main_token_symbol, 
        paired1_token as paired_token,
        paired1_token_symbol as paired_token_symbol,
        main_token_reserve, paired1_token_reserve as paired_token_reserve,
        main_token_usd_price,
        paired1_token_usd_price as paired_token_usd_price,
        coalesce(trading_volume.trading_volume,0) as trading_volume
from reserves r
left join trading_volume on r.pool_id = trading_volume.poolId and  r.day = trading_volume.time
order by day desc, pool_id
)


select 
blockchain ||' '|| project ||' '|| coalesce(paired_token_symbol,'unknown') ||':'|| main_token_symbol ||'('|| substring(cast(pool as varchar),64) ||')' as pool_name,
* 
from all_metrics
