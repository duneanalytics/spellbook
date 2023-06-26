{{ config(
    schema='lido_liquidity_arbitrum',
    alias = 'balancer_pools',
    partition_by = ['time'],
    materialized = 'table',
    file_format = 'delta',
    unique_key = ['pool', 'time'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "lido_liquidity",
                                \'["ppclunghe"]\') }}'
    )
}}

{% set project_start_date = '2022-09-17' %} 

with dates AS (
        SELECT explode(sequence(to_date('{{ project_start_date }}'), now(), interval 1 day)) AS day
    )
 

, pools as (   
SELECT
    registered.poolId AS pool_id,
    tokens.token_address
FROM {{source('balancer_v2_arbitrum','Vault_evt_PoolRegistered')}} registered
INNER JOIN {{source('balancer_v2_arbitrum','WeightedPoolV2Factory_call_create')}} call_create
    ON call_create.output_0 = SUBSTRING(registered.poolId, 0, 42)
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE tokens.token_address = lower('0x5979d7b546e38e414f7e9822514be443a4800529')
union all
SELECT
    registered.poolId AS pool_id,
    tokens.token_address
FROM {{source('balancer_v2_arbitrum','Vault_evt_PoolRegistered')}} registered
INNER JOIN {{source('balancer_v2_arbitrum','WeightedPool2TokensFactory_call_create')}} call_create
    ON call_create.output_0 = SUBSTRING(registered.poolId, 0, 42)
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE tokens.token_address = lower('0x5979d7b546e38e414f7e9822514be443a4800529')
union all
SELECT
    registered.poolId AS pool_id,
    tokens.token_address
FROM {{source('balancer_v2_arbitrum','Vault_evt_PoolRegistered')}} registered
INNER JOIN {{source('balancer_v2_arbitrum','WeightedPoolFactory_call_create')}} call_create
    ON call_create.output_0 = SUBSTRING(registered.poolId, 0, 42)
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE tokens.token_address = lower('0x5979d7b546e38e414f7e9822514be443a4800529')
union all
SELECT
    registered.poolId AS pool_id,
    tokens.token_address
FROM {{source('balancer_v2_arbitrum','Vault_evt_PoolRegistered')}} registered
INNER JOIN {{source('balancer_v2_arbitrum','StablePoolFactory_call_create')}} call_create
    ON call_create.output_0 = SUBSTRING(registered.poolId, 0, 42)
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE tokens.token_address = lower('0x5979d7b546e38e414f7e9822514be443a4800529')
union all
SELECT
    registered.poolId AS pool_id,
    tokens.token_address
FROM {{source('balancer_v2_arbitrum','Vault_evt_PoolRegistered')}} registered
INNER JOIN  {{source('balancer_v2_arbitrum','MetaStablePoolFactory_call_create')}} call_create
    ON call_create.output_0 = SUBSTRING(registered.poolId, 0, 42)
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE tokens.token_address = lower('0x5979d7b546e38e414f7e9822514be443a4800529')
union all
SELECT
    registered.poolId AS pool_id,
    tokens.token_address
FROM {{source('balancer_v2_arbitrum','Vault_evt_PoolRegistered')}} registered
INNER JOIN  {{source('balancer_v2_arbitrum','ComposableStablePoolFactory_call_create')}} call_create
    ON call_create.output_0 = SUBSTRING(registered.poolId, 0, 42)
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE tokens.token_address = lower('0x5979d7b546e38e414f7e9822514be443a4800529')
union all
SELECT
    registered.poolId AS pool_id,
    tokens.token_address
FROM {{source('balancer_v2_arbitrum','Vault_evt_PoolRegistered')}} registered
INNER JOIN  {{source('balancer_v2_arbitrum','LiquidityBootstrappingPoolFactory_call_create')}} call_create
    ON call_create.output_0 = SUBSTRING(registered.poolId, 0, 42)
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE tokens.token_address = lower('0x5979d7b546e38e414f7e9822514be443a4800529')

)

, pool_per_date as ( 
select dates.day, pools.*
from dates
left join pools on 1=1
)

, pools_fee as (
select  contract_address, block_time as time, lead(block_time,1,now()) over (partition by contract_address order by contract_address, block_time) as next_time, swap_fee_percentage/1e18 as fee 
from {{ref('balancer_v2_arbitrum_pools_fees')}}
where contract_address in (select SUBSTRING(pool_id, 0, 42) from pools)
)

, tokens as (
select distinct token_address from (
SELECT  tokens.token_address
FROM {{source('balancer_v2_arbitrum','WeightedPoolV2Factory_call_create')}} call_create
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE call_create.output_0 in (select distinct  SUBSTRING(pool_id, 0, 42) from pools)
union all
SELECT tokens.token_address
FROM {{source('balancer_v2_arbitrum','WeightedPool2TokensFactory_call_create')}} call_create
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE call_create.output_0 in (select distinct  SUBSTRING(pool_id, 0, 42) from pools)
union all
SELECT tokens.token_address
FROM {{source('balancer_v2_arbitrum','WeightedPoolFactory_call_create')}} call_create
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE call_create.output_0 in (select distinct  SUBSTRING(pool_id, 0, 42) from pools)
union all
SELECT tokens.token_address
FROM {{source('balancer_v2_arbitrum','StablePoolFactory_call_create')}} call_create
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE call_create.output_0 in (select distinct  SUBSTRING(pool_id, 0, 42) from pools)
union all
SELECT tokens.token_address
FROM {{source('balancer_v2_arbitrum','MetaStablePoolFactory_call_create')}} call_create
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE call_create.output_0 in (select distinct  SUBSTRING(pool_id, 0, 42) from pools)
union all
SELECT tokens.token_address
FROM {{source('balancer_v2_arbitrum','ComposableStablePoolFactory_call_create')}} call_create
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE call_create.output_0 in (select distinct  SUBSTRING(pool_id, 0, 42) from pools)
union all
SELECT tokens.token_address
FROM {{source('balancer_v2_arbitrum','LiquidityBootstrappingPoolFactory_call_create')}} call_create
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE call_create.output_0 in (select distinct  SUBSTRING(pool_id, 0, 42) from pools)
))

, tokens_prices_daily AS (
    SELECT distinct
        DATE_TRUNC('day', minute) AS time,
        contract_address as token,
        symbol,
        decimals,
        avg(price) AS price
    FROM {{source('prices','usd')}}
    WHERE date_trunc('day', minute) >= '{{ project_start_date }}' and date_trunc('day', minute) < date_trunc('day', now())
    and blockchain = 'arbitrum'
    and contract_address in (select distinct token_address from tokens)
    group by 1,2,3,4
    union all
    SELECT distinct
        DATE_TRUNC('day', minute), 
        contract_address as token,
        symbol,
        decimals,
        last_value(price) over (partition by DATE_TRUNC('day', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{source('prices','usd')}}
    WHERE date_trunc('day', minute) = date_trunc('day', now())
    and blockchain = 'arbitrum'
    and contract_address in (select distinct token_address from tokens)
)

, wsteth_prices_hourly AS (
    select time, lead(time,1, DATE_TRUNC('hour', now() + interval '1' hour)) over (order by time) as next_time, price
    from (
    SELECT distinct
        DATE_TRUNC('hour', minute) time,
        last_value(price) over (partition by DATE_TRUNC('hour', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{source('prices','usd')}}
    WHERE date_trunc('day', minute) >= '{{ project_start_date }}' 
    and blockchain = 'arbitrum'
    and contract_address = lower('0x5979d7b546e38e414f7e9822514be443a4800529')
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
                    amountIn AS delta
                FROM {{source('balancer_v2_arbitrum','Vault_evt_Swap')}}
                WHERE date_trunc('day', evt_block_time) >= '{{ project_start_date }}'
                and poolId in (select pool_id from pools)
                UNION
                ALL
                SELECT
                    date_trunc('day', evt_block_time) AS day,
                    poolId AS pool_id,
                    tokenOut AS token,
                    -amountOut AS delta
                FROM {{source('balancer_v2_arbitrum','Vault_evt_Swap')}}
                WHERE date_trunc('day', evt_block_time) >= '{{ project_start_date }}'
                and poolId in (select pool_id from pools)
            ) swaps
        GROUP BY 1, 2, 3
    )

, zipped_balance_changes AS (
        SELECT
            date_trunc('day', evt_block_time) AS day,
            poolId AS pool_id,
            explode(arrays_zip(tokens, deltas, protocolFeeAmounts)) AS zipped
        FROM {{source('balancer_v2_arbitrum','Vault_evt_PoolBalanceChanged')}}
        WHERE date_trunc('day', evt_block_time) >= '{{ project_start_date }}'
        and poolId in (select pool_id from pools)
)

 , balances_changes AS (
        SELECT
            day,
            pool_id,
            zipped.tokens AS token,
            zipped.deltas - zipped.protocolFeeAmounts AS delta
        FROM zipped_balance_changes
        ORDER BY 1, 2, 3
    )

, managed_changes AS (
        SELECT
            date_trunc('day', evt_block_time) AS day,
            poolId AS pool_id,
            token,
            cashDelta + managedDelta AS delta
        FROM {{source('balancer_v2_arbitrum','Vault_evt_PoolBalanceManaged')}}
        WHERE date_trunc('day', evt_block_time) >= '{{ project_start_date }}'
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
            COALESCE(t.symbol, p1.symbol) AS token_symbol,
            cumulative_amount as token_balance_raw,
            cumulative_amount / POWER(10, COALESCE(t.decimals, p1.decimals)) AS token_balance,
            cumulative_amount / POWER(10, COALESCE(t.decimals, p1.decimals)) * COALESCE(p1.price, 0) AS amount_usd, 
            0 as row_numb
        FROM  pool_per_date  c
        LEFT JOIN cumulative_balance b ON c.pool_id = b.pool_id and  b.day <= c.day AND c.day < b.day_of_next_change
        LEFT JOIN {{source('prices','tokens')}} t ON t.contract_address = b.token AND blockchain = "arbitrum"
        LEFT JOIN tokens_prices_daily p1 ON p1.time = b.day AND p1.token = b.token
        WHERE b.token = '0x5979d7b546e38e414f7e9822514be443a4800529'
        union all
        SELECT
            c.day,
            c.pool_id,
            b.token,
            COALESCE(t.symbol, p1.symbol) AS token_symbol,
            cumulative_amount as token_balance_raw,
            cumulative_amount / POWER(10, COALESCE(t.decimals, p1.decimals)) AS token_balance,
            cumulative_amount / POWER(10, COALESCE(t.decimals, p1.decimals)) * COALESCE(p1.price, 0) AS amount_usd, 
            row_number() OVER(PARTITION BY c.day, c.pool_id ORDER BY  c.day, c.pool_id, b.token) as row_numb
        FROM  pool_per_date  c
        LEFT JOIN cumulative_balance b ON c.pool_id = b.pool_id and b.day <= c.day AND c.day < b.day_of_next_change
        LEFT JOIN {{source('prices','tokens')}} t ON t.contract_address = (case when b.token = lower('0xda1cd1711743e57dd57102e9e61b75f3587703da') then lower('0x82aF49447D8a07e3bd95BD0d56f35241523fBab1') else b.token end) AND blockchain = "arbitrum"
        LEFT JOIN tokens_prices_daily p1 ON p1.time = b.day AND p1.token = (case when b.token = lower('0xda1cd1711743e57dd57102e9e61b75f3587703da') then lower('0x82aF49447D8a07e3bd95BD0d56f35241523fBab1') else b.token end)
        WHERE b.token != '0x5979d7b546e38e414f7e9822514be443a4800529' and b.token !=SUBSTRING(b.pool_id, 0, 42)
)


, reserves as (
select main.day, main.pool_id, 
main_token, main_token_symbol, main_token_reserve, main_token_usd_reserve,
paired1_token, paired1_token_symbol, paired1_token_reserve, paired1_token_usd_reserve,
paired2_token, paired2_token_symbol, paired2_token_reserve, paired2_token_usd_reserve,
paired3_token, paired3_token_symbol, paired3_token_reserve, paired3_token_usd_reserve,
paired4_token, paired4_token_symbol, paired4_token_reserve, paired4_token_usd_reserve
from (
SELECT
    b.day,
    b.pool_id,
    token AS main_token,
    token_symbol as main_token_symbol,
    coalesce(token_balance, token_balance_raw) as main_token_reserve,
    coalesce(amount_usd, 0) AS main_token_usd_reserve
FROM cumulative_usd_balance b
where token = lower('0x5979d7b546e38e414f7e9822514be443a4800529') ) main
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
where token != lower('0x5979d7b546e38e414f7e9822514be443a4800529') and cast(row_numb as int) = cast(1 as int)) paired1
on main.day = paired1.day and main.pool_id = paired1.pool_id 
left join (
SELECT
    b.day,
    b.pool_id,
    token AS paired2_token,
    token_symbol as paired2_token_symbol,
    coalesce(token_balance, token_balance_raw) as paired2_token_reserve,
    coalesce(amount_usd, 0) AS paired2_token_usd_reserve
FROM cumulative_usd_balance b
where token != lower('0x5979d7b546e38e414f7e9822514be443a4800529') and cast(row_numb as int) = cast(2 as int)) paired2
on main.day = paired2.day and main.pool_id = paired2.pool_id 
left join (
SELECT
    b.day,
    b.pool_id,
    token AS paired3_token,
    token_symbol as paired3_token_symbol,
    coalesce(token_balance, token_balance_raw) as paired3_token_reserve,
    coalesce(amount_usd, 0) AS paired3_token_usd_reserve
FROM cumulative_usd_balance b
where token != lower('0x5979d7b546e38e414f7e9822514be443a4800529') and cast(row_numb as int) = cast(3 as int)) paired3
on main.day = paired3.day and main.pool_id = paired3.pool_id 
left join (
SELECT
    b.day,
    b.pool_id,
    token AS paired4_token,
    token_symbol as paired4_token_symbol,
    coalesce(token_balance, token_balance_raw) as paired4_token_reserve,
    coalesce(amount_usd, 0) AS paired4_token_usd_reserve
FROM cumulative_usd_balance b
where token != lower('0x5979d7b546e38e414f7e9822514be443a4800529') and cast(row_numb as int) = cast(4 as int)) paired4
on main.day = paired4.day and main.pool_id = paired4.pool_id 
)

, trading_volume as (    
    select  date_trunc('day', s.evt_block_time) as time,
        poolId,
        sum(case when tokenOut = lower('0x5979d7b546e38e414f7e9822514be443a4800529') then p.price*amountOut/1e18 
             else p.price*amountIn/1e18 end) as trading_volume
    from {{source('balancer_v2_arbitrum','Vault_evt_Swap')}} s
    left join wsteth_prices_hourly p on date_trunc('hour', s.evt_block_time) >= p.time and date_trunc('hour', s.evt_block_time) < p.next_time
    WHERE date_trunc('day', s.evt_block_time) >= '{{ project_start_date }}'
    and s.poolId in (select pool_id from pools)
    group by 1,2
) 


, all_metrics as (
select  pool_id as pool, 'arbitrum' as blockchain, 'balancer' as project, pools_fee.fee, day as time, main_token, main_token_symbol, 
paired1_token||decode(paired2_token, null, '', '/'||coalesce(paired2_token,''))||decode(paired3_token, null, '', '/'||coalesce(paired3_token,''))||decode(paired4_token, null, '', '/'||coalesce(paired4_token,'')) as paired_token,
paired1_token_symbol||decode(paired2_token_symbol, null, '', '/'||coalesce(paired2_token_symbol,''))||decode(paired3_token_symbol, null, '', '/'||coalesce(paired3_token_symbol,''))||decode(paired4_token_symbol, null, '', '/'||coalesce(paired4_token_symbol,'')) as paired_token_symbol,
main_token_reserve, paired1_token_reserve as paired_token_reserve,
main_token_usd_reserve,
paired1_token_usd_reserve + coalesce(paired2_token_usd_reserve,0) + coalesce(paired3_token_usd_reserve,0) + coalesce(paired4_token_usd_reserve,0) as paired_token_usd_reserve,
coalesce(trading_volume.trading_volume,0) as trading_volume
from reserves r
left join trading_volume on r.pool_id = trading_volume.poolId and  r.day = trading_volume.time
left join pools_fee on SUBSTRING(r.pool_id, 0, 42) = pools_fee.contract_address and r.day >= pools_fee.time and r.day < pools_fee.next_time
order by day desc, pool_id
)


select CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(blockchain,CONCAT(' ', project)) ,' '), coalesce(paired_token_symbol,paired_token)),':', main_token_symbol, '(', cast(pool as varchar(45)), ')')) as pool_name,* 
from all_metrics
where main_token_reserve > 1
