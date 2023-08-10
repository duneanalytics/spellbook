{{ config(
    schema='lido_liquidity_polygon', 
    alias = alias('balancer_pools'), 
    tags = ['dunesql'], 
    partition_by = ['time'],
    materialized = 'table',
    file_format = 'delta',
    unique_key = ['pool', 'time'],
    post_hook='{{ expose_spells(\'["polygon"]\', 
                                "project",
                                "lido_liquidity",
                                \'["ppclunghe", "kemasan"]\') }}' 
    )
}}

{% set project_start_date = '2023-03-02' %} 


with dates as (
    with day_seq as (select (sequence(cast('{{ project_start_date }}' as date), current_date, interval '1' day)) as day)

select days.day
from day_seq
cross join unnest(day) as days(day)
  )

, pools(pool_id,poolAddress) as (  
values    
(0x4a77ef015ddcd972fd9ba2c7d5d658689d090f1a000000000000000000000b38, 0x4a77ef015ddcd972fd9ba2c7d5d658689d090f1a),
(0x65fe9314be50890fb01457be076fafd05ff32b9a000000000000000000000a96, 0x65fe9314be50890fb01457be076fafd05ff32b9a)
)

, pool_per_date as ( 
select dates.day, pools.*
from dates
left join pools on 1=1
)

, tokens as (
select distinct token_address from (
SELECT  tokens.token_address
FROM {{source ('balancer_v2_polygon','WeightedPoolV2Factory_call_create')}} call_create
     CROSS JOIN UNNEST(call_create.tokens) as tokens(token_address)
WHERE call_create.output_0 in (select distinct  poolAddress from pools)
union all
SELECT tokens.token_address
FROM {{source ('balancer_v2_polygon','WeightedPool2TokensFactory_call_create')}} call_create
     CROSS JOIN UNNEST(call_create.tokens) as tokens(token_address)
WHERE call_create.output_0 in (select distinct  poolAddress from pools)
union all
SELECT tokens.token_address
FROM {{source ('balancer_v2_polygon','WeightedPoolFactory_call_create')}} call_create
     CROSS JOIN UNNEST(call_create.tokens) as tokens(token_address)
WHERE call_create.output_0 in (select distinct  poolAddress from pools)
union all
SELECT tokens.token_address
FROM {{source ('balancer_v2_polygon','StablePoolFactory_call_create')}} call_create
     CROSS JOIN UNNEST(call_create.tokens) as tokens(token_address)
WHERE call_create.output_0 in (select distinct  poolAddress from pools)
union all
SELECT tokens.token_address
FROM {{source ('balancer_v2_polygon','MetaStablePoolFactory_call_create')}} call_create
     CROSS JOIN UNNEST(call_create.tokens) as tokens(token_address)
WHERE call_create.output_0 in (select distinct  poolAddress from pools)
union all
SELECT tokens.token_address
FROM {{source ('balancer_v2_polygon','ComposableStablePoolFactory_call_create')}} call_create
     CROSS JOIN UNNEST(call_create.tokens) as tokens(token_address)
WHERE call_create.output_0 in (select distinct  poolAddress from pools)
union all
SELECT tokens.token_address
FROM {{source ('balancer_v2_polygon','LiquidityBootstrappingPoolFactory_call_create')}} call_create
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
    FROM {{source ('prices','usd')}}
    WHERE date_trunc('day', minute) >= date '{{ project_start_date }}' 
    and date_trunc('day', minute) < current_date
    and blockchain = 'polygon'
    and contract_address in (select distinct token_address from tokens)
    group by 1,2,3,4
    union all
    SELECT distinct
        DATE_TRUNC('day', minute), 
        contract_address as token,
        symbol,
        decimals,
        last_value(price) over (partition by DATE_TRUNC('day', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{source ('prices','usd')}}
    WHERE date_trunc('day', minute) = current_date
    and blockchain = 'polygon'
    and contract_address in (select distinct token_address from tokens)
    union all
SELECT distinct
        DATE_TRUNC('day', minute) AS time,
        0x43894DE14462B421372bCFe445fA51b1b4A0Ff3D as token,
        'bb-a-WETH',
        18,
        avg(price) AS price
    FROM {{source ('prices','usd')}}
    WHERE date_trunc('day', minute) >= date '{{ project_start_date }}' 
    and date_trunc('day', minute) < current_date
    and blockchain = 'polygon'
    and contract_address = 0x7ceb23fd6bc0add59e62ac25578270cff1b9f619
    group by 1,2,3
union all
    SELECT distinct
        DATE_TRUNC('day', minute), 
        0x43894DE14462B421372bCFe445fA51b1b4A0Ff3D as token, --bb-a-WETH
        'bb-a-WETH',
        18,
        last_value(price) over (partition by DATE_TRUNC('day', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{source ('prices','usd')}}
    WHERE date_trunc('day', minute) = current_date
    and blockchain = 'polygon'
    and contract_address = 0x7ceb23fd6bc0add59e62ac25578270cff1b9f619
)

, wsteth_prices_hourly AS (
    select time, lead(time,1, DATE_TRUNC('hour', now() + interval '1' hour)) over (order by time) as next_time, price
    from (
    SELECT distinct
        DATE_TRUNC('hour', minute) time,
        last_value(price) over (partition by DATE_TRUNC('hour', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{source ('prices','usd')}}
    WHERE date_trunc('day', minute) >= date '{{ project_start_date }}' 
    and blockchain = 'polygon'
    and contract_address = 0x03b54A6e9a984069379fae1a4fC4dBAE93B3bCCD
))

, weth_prices_hourly AS (
    select time
    , lead(time,1, DATE_TRUNC('hour', now() + interval '1' hour)) over (order by time) as next_time
    , price
    from (
    SELECT distinct
        DATE_TRUNC('hour', minute) time
        , last_value(price) over (partition by DATE_TRUNC('hour', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{source ('prices','usd')}}
    WHERE date_trunc('day', minute) >= date '{{ project_start_date }}' 
    and blockchain = 'polygon'
    and contract_address = 0x7ceb23fd6bc0add59e62ac25578270cff1b9f619
))

,  bb_a_weth_prices_hourly AS (
    select time
    , lead(time,1, DATE_TRUNC('hour', now() + interval '1' hour)) over (order by time) as next_time
    , price
    from (
    SELECT distinct
        DATE_TRUNC('hour', minute) AS time,
        0x43894DE14462B421372bCFe445fA51b1b4A0Ff3D as token,
        last_value(price) over (partition by DATE_TRUNC('hour', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{source ('prices','usd')}}
    WHERE date_trunc('day', minute) >= date '{{ project_start_date }}' 
    and blockchain = 'polygon'
    and contract_address = 0x7ceb23fd6bc0add59e62ac25578270cff1b9f619
    
)
)
, swaps_changes as (
        SELECT
            d. day,
            pool_id,
            token,
            SUM(COALESCE(delta, 0)) AS delta
        FROM dates d left join 
            (
                SELECT
                    date_trunc('day', evt_block_time) AS day,
                    poolId AS pool_id,
                    tokenIn AS token,
                    cast(amountIn as double) AS delta
                FROM {{source ('balancer_v2_polygon','Vault_evt_Swap')}}
                WHERE date_trunc('day', evt_block_time) >= date '{{ project_start_date }}' 
                and poolId in (select pool_id from pools)
                UNION
                ALL
                SELECT
                    date_trunc('day', evt_block_time) AS day,
                    poolId AS pool_id,
                    tokenOut AS token,
                    -cast(amountOut as double) AS delta
                FROM {{source ('balancer_v2_polygon','Vault_evt_Swap')}}
                WHERE date_trunc('day', evt_block_time) >= date '{{ project_start_date }}' 
                and poolId in (select pool_id from pools)
            ) swaps on d.day = swaps.day
        GROUP BY 1, 2, 3
    )
    
, balances_changes AS (
        SELECT
            date_trunc('day', evt_block_time) AS day,
            poolId AS pool_id,
            u.token,
            cast(u.delta as double) as delta
        FROM {{source('balancer_v2_polygon','Vault_evt_PoolBalanceChanged')}}
         CROSS JOIN UNNEST(tokens, deltas) as u(token, delta)
        WHERE date_trunc('day', evt_block_time) >= date '{{ project_start_date }}'
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
        FROM {{source ('balancer_v2_polygon','Vault_evt_PoolBalanceManaged')}}
        WHERE date_trunc('day', evt_block_time) >= date '{{ project_start_date }}' 
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

, cumulative_balance AS (
        SELECT
            DAY,
            pool_id,
            token,
            LEAD(DAY, 1, NOW()) OVER (PARTITION BY token, pool_id ORDER BY DAY) AS day_of_next_change,
            SUM(amount) OVER (PARTITION BY pool_id, token ORDER BY DAY ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
        FROM daily_delta_balance
)


, cumulative_usd_balance AS (
        SELECT
            c.day,
            c.pool_id,
            b.token,
            p1.symbol AS token_symbol,
            cumulative_amount as token_balance_raw,
            cumulative_amount / POWER(10, p1.decimals) AS token_balance,
            cumulative_amount / POWER(10, p1.decimals) * COALESCE(p1.price, 0) AS amount_usd, 
            0 as row_numb
        FROM   pool_per_date c 
        LEFT JOIN cumulative_balance b ON b.day <= c.day AND c.day < b.day_of_next_change and c.pool_id = b.pool_id
        LEFT JOIN tokens_prices_daily p1 ON p1.time = b.day AND p1.token = b.token
        WHERE  b.token in (0x03b54a6e9a984069379fae1a4fc4dbae93b3bccd)
        union all
        SELECT
            c.day,
            c.pool_id,
            b.token,
            p1.symbol AS token_symbol,
            cumulative_amount as token_balance_raw,
            cumulative_amount / POWER(10, p1.decimals) AS token_balance,
            cumulative_amount / POWER(10, p1.decimals) * COALESCE(p1.price, 0) AS amount_usd, 
            row_number() OVER(PARTITION BY c.day,c.pool_id ORDER BY  c.day,c.pool_id, b.token) as row_numb
        FROM   pool_per_date c 
        LEFT JOIN cumulative_balance b ON b.day <= c.day AND c.day < b.day_of_next_change and c.pool_id = b.pool_id
        LEFT JOIN tokens_prices_daily p1 ON p1.time = b.day AND p1.token = b.token
        WHERE b.token not in (select distinct poolAddress from pools
                            union all
                            select 0x03b54a6e9a984069379fae1a4fc4dbae93b3bccd
                            )
)


, reserves as (
select main.day, main.pool_id, 
main_token, main_token_symbol, main_token_reserve, main_token_usd_reserve,
paired1_token, paired1_token_symbol, paired1_token_reserve, paired1_token_usd_reserve,
paired2_token, paired2_token_symbol, paired2_token_reserve, paired2_token_usd_reserve
--paired3_token, paired3_token_symbol, paired3_token_reserve, paired3_token_usd_reserve,
--paired4_token, paired4_token_symbol, paired4_token_reserve, paired4_token_usd_reserve,
--paired5_token, paired5_token_symbol, paired5_token_reserve, paired5_token_usd_reserve,
--paired6_token, paired6_token_symbol, paired6_token_reserve, paired6_token_usd_reserve,
--paired7_token, paired7_token_symbol, paired7_token_reserve, paired7_token_usd_reserve


from (

SELECT
    b.day,
    b.pool_id,
    token AS main_token,
    token_symbol as main_token_symbol,
    coalesce(token_balance, token_balance_raw) as main_token_reserve,
    coalesce(amount_usd, 0) AS main_token_usd_reserve
FROM cumulative_usd_balance b
where token = 0x03b54A6e9a984069379fae1a4fC4dBAE93B3bCCD) main

join 
(
SELECT
    b.day,
    b.pool_id,
    token AS paired1_token,
    token_symbol as paired1_token_symbol,
    coalesce(token_balance, token_balance_raw) as paired1_token_reserve,
    coalesce(amount_usd, 0) AS paired1_token_usd_reserve
FROM cumulative_usd_balance b
where token not in (0x03b54a6e9a984069379fae1a4fc4dbae93b3bccd) 
  and cast(row_numb as int) = int '1'
)paired1
on main.day = paired1.day and main.pool_id = paired1.pool_id 

left join 
(
SELECT
    b.day,
    b.pool_id,
    token AS paired2_token,
    token_symbol as paired2_token_symbol,
    coalesce(token_balance, token_balance_raw) as paired2_token_reserve,
    coalesce(amount_usd, 0) AS paired2_token_usd_reserve
FROM cumulative_usd_balance b
where token not in (0x03b54a6e9a984069379fae1a4fc4dbae93b3bccd) 
  and cast(row_numb as int) = int '2') paired2
on main.day = paired2.day and main.pool_id = paired2.pool_id 
/*
left join 
(
SELECT
    b.day,
    b.pool_id,
    token AS paired3_token,
    token_symbol as paired3_token_symbol,
    coalesce(token_balance, token_balance_raw) as paired3_token_reserve,
    coalesce(amount_usd, 0) AS paired3_token_usd_reserve
FROM cumulative_usd_balance b
where token != lower('0x03b54A6e9a984069379fae1a4fC4dBAE93B3bCCD') and cast(row_numb as int) = cast(3 as int)) paired3
on main.day = paired3.day and main.pool_id = paired3.pool_id 

left join 
(
SELECT
    b.day,
    b.pool_id,
    token AS paired4_token,
    token_symbol as paired4_token_symbol,
    coalesce(token_balance, token_balance_raw) as paired4_token_reserve,
    coalesce(amount_usd, 0) AS paired4_token_usd_reserve
FROM cumulative_usd_balance b
where token != lower('0x03b54A6e9a984069379fae1a4fC4dBAE93B3bCCD') and cast(row_numb as int) = cast(4 as int)) paired4
on main.day = paired4.day and main.pool_id = paired4.pool_id 

left join 
(
SELECT
    b.day,
    b.pool_id,
    token AS paired5_token,
    token_symbol as paired5_token_symbol,
    coalesce(token_balance, token_balance_raw) as paired5_token_reserve,
    coalesce(amount_usd, 0) AS paired5_token_usd_reserve
FROM cumulative_usd_balance b
where token != lower('0x03b54A6e9a984069379fae1a4fC4dBAE93B3bCCD') and cast(row_numb as int) = cast(5 as int)) paired5
on main.day = paired5.day and main.pool_id = paired5.pool_id 

left join 
(
SELECT
    b.day,
    b.pool_id,
    token AS paired6_token,
    token_symbol as paired6_token_symbol,
    coalesce(token_balance, token_balance_raw) as paired6_token_reserve,
    coalesce(amount_usd, 0) AS paired6_token_usd_reserve
FROM cumulative_usd_balance b
where token != lower('0x03b54A6e9a984069379fae1a4fC4dBAE93B3bCCD') and cast(row_numb as int) = cast(6 as int)) paired6
on main.day = paired6.day and main.pool_id = paired6.pool_id 

left join 
(
SELECT
    b.day,
    b.pool_id,
    token AS paired7_token,
    token_symbol as paired7_token_symbol,
    coalesce(token_balance, token_balance_raw) as paired7_token_reserve,
    coalesce(amount_usd, 0) AS paired7_token_usd_reserve
FROM cumulative_usd_balance b
where token != lower('0x03b54A6e9a984069379fae1a4fC4dBAE93B3bCCD') and cast(row_numb as int) = cast(7 as int)) paired7
on main.day = paired7.day and main.pool_id = paired7.pool_id 
*/
order by 1 desc

)

, trading_volume as (    
    select  date_trunc('day', s.evt_block_time) as time,
        poolId,
        sum(case when tokenOut in (0x03b54a6e9a984069379fae1a4fc4dbae93b3bccd, 0x43894DE14462B421372bCFe445fA51b1b4A0Ff3D)  
            then p.price*amountOut/1e18 
            else p.price*amountIn/1e18 end) as trading_volume
    from {{source('balancer_v2_polygon','Vault_evt_Swap')}} s
    left join wsteth_prices_hourly p on date_trunc('hour', s.evt_block_time) >= p.time and date_trunc('hour', s.evt_block_time) < p.next_time
    WHERE date_trunc('day', evt_block_time) >= date '{{ project_start_date }}' 
    and s.poolId in (select pool_id from pools)
    group by 1,2
    
) 

, all_metrics as (
select  pool_id as pool, 'polygon' as blockchain, 'balancer' as project, 0 as fee, 
cast(day as date) as time, main_token, main_token_symbol, 
paired1_token as paired_token,
--||decode(paired2_token, null, '', '/'||coalesce(paired2_token,''))||decode(paired3_token, null, '', '/'||coalesce(paired3_token,''))||decode(paired4_token, null, '', '/'||coalesce(paired4_token,'')) as paired_token,
paired1_token_symbol||case when paired2_token_symbol is null then '' else '/'||paired2_token_symbol end as paired_token_symbol,
--||decode(paired2_token_symbol, null, '', '/'||coalesce(paired2_token_symbol,''))||decode(paired3_token_symbol, null, '', '/'||coalesce(paired3_token_symbol,''))||decode(paired4_token_symbol, null, '', '/'||coalesce(paired4_token_symbol,'')) as paired_token_symbol,
main_token_reserve, paired1_token_reserve as paired_token_reserve,
main_token_usd_reserve,
paired1_token_usd_reserve + coalesce(paired2_token_usd_reserve,0) as paired_token_usd_reserve,
--+ coalesce(paired3_token_usd_reserve,0) + coalesce(paired4_token_usd_reserve,0) as paired_token_usd_reserve,
coalesce(trading_volume.trading_volume,0) as trading_volume
from reserves r
left join trading_volume on r.pool_id = trading_volume.poolId and  r.day = trading_volume.time
--left join pools_fee on r.poolAddress = pools_fee.contract_address and r.day >= pools_fee.time and r.day < pools_fee.next_time
order by day desc, pool_id
)


select 
blockchain ||' '|| project ||' '|| coalesce(paired_token_symbol,'unknown') ||':'|| main_token_symbol ||'('|| substring(cast(pool as varchar),64) ||')' as pool_name,
* 
from all_metrics
where main_token_reserve > 1