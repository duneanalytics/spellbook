{{ config(
    schema='lido_liquidity_optimism',
    alias = 'balancer_pools',
    partition_by = ['time'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool', 'time'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "lido_liquidity",
                                \'["ppclunghe"]\') }}'
    )
}}

{% set project_start_date = '2022-09-17' %}

with dates as (
select explode(sequence(to_date('{{ project_start_date }}'), now(), interval 1 day)) as day
)


, pools as (   
SELECT
    registered.poolId AS pool_id,
    tokens.token_address, 
    evt_block_time
FROM {{ source('balancer_v2_optimism','Vault_evt_PoolRegistered')}}  registered
INNER JOIN {{ source('balancer_v2_optimism','WeightedPoolV2Factory_call_create')}} call_create
    ON call_create.output_0 = SUBSTRING(registered.poolId, 0, 42)
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE tokens.token_address = lower('0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb')
union all
SELECT
    registered.poolId AS pool_id,
    tokens.token_address,
    evt_block_time
FROM {{ source('balancer_v2_optimism','Vault_evt_PoolRegistered')}} registered
INNER JOIN {{ source('balancer_v2_optimism','WeightedPool2TokensFactory_call_create')}} call_create
    ON call_create.output_0 = SUBSTRING(registered.poolId, 0, 42)
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE tokens.token_address = lower('0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb')
union all
SELECT
    registered.poolId AS pool_id,
    tokens.token_address,
    evt_block_time
FROM {{ source('balancer_v2_optimism','Vault_evt_PoolRegistered')}} registered
INNER JOIN {{ source('balancer_v2_optimism','WeightedPoolFactory_call_create')}} call_create
    ON call_create.output_0 = SUBSTRING(registered.poolId, 0, 42)
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE tokens.token_address = lower('0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb')
union all
SELECT
    registered.poolId AS pool_id,
    tokens.token_address,
    evt_block_time
FROM {{ source('balancer_v2_optimism','Vault_evt_PoolRegistered')}} registered
INNER JOIN {{ source('balancer_v2_optimism','StablePoolFactory_call_create')}} call_create
    ON call_create.output_0 = SUBSTRING(registered.poolId, 0, 42)
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE tokens.token_address = lower('0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb')
union all
SELECT
    registered.poolId AS pool_id,
    tokens.token_address,
    evt_block_time
FROM {{ source('balancer_v2_optimism','Vault_evt_PoolRegistered')}} registered
INNER JOIN  {{ source('balancer_v2_optimism','MetaStablePoolFactory_call_create')}} call_create
    ON call_create.output_0 = SUBSTRING(registered.poolId, 0, 42)
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE tokens.token_address = lower('0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb')
union all
SELECT
    registered.poolId AS pool_id,
    tokens.token_address,
    evt_block_time
FROM {{ source('balancer_v2_optimism','Vault_evt_PoolRegistered')}} registered
INNER JOIN  {{ source('balancer_v2_optimism','ComposableStablePoolFactory_call_create')}} call_create
    ON call_create.output_0 = SUBSTRING(registered.poolId, 0, 42)
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE tokens.token_address = lower('0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb')
union all
select '0x098f32d98d0d64dba199fc1923d3bf4192e787190001000000000000000000d2', lower('0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb'), cast('2023-04-28' as timestamp)

)


, pools_fee as (
select  contract_address, block_time as time, lead(block_time,1,now()) over (partition by contract_address order by contract_address, block_time) as next_time, swap_fee_percentage/1e18 as fee 
from {{ref('balancer_v2_optimism_pools_fees')}}
where contract_address in (select SUBSTRING(pool_id, 0, 42) from pools)
)

, tokens as (
select distinct token_address from (
SELECT  tokens.token_address
FROM {{ source('balancer_v2_optimism','WeightedPoolV2Factory_call_create')}} call_create
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE call_create.output_0 in (select distinct  SUBSTRING(pool_id, 0, 42) from pools)
union all
SELECT tokens.token_address
FROM {{ source('balancer_v2_optimism','WeightedPool2TokensFactory_call_create')}} call_create
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE call_create.output_0 in (select distinct  SUBSTRING(pool_id, 0, 42) from pools)
union all
SELECT tokens.token_address
FROM {{ source('balancer_v2_optimism','WeightedPoolFactory_call_create')}} call_create
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE call_create.output_0 in (select distinct  SUBSTRING(pool_id, 0, 42) from pools)
union all
SELECT tokens.token_address
FROM {{ source('balancer_v2_optimism','StablePoolFactory_call_create')}} call_create
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE call_create.output_0 in (select distinct  SUBSTRING(pool_id, 0, 42) from pools)
union all
SELECT tokens.token_address
FROM {{ source('balancer_v2_optimism','MetaStablePoolFactory_call_create')}} call_create
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
WHERE call_create.output_0 in (select distinct  SUBSTRING(pool_id, 0, 42) from pools)
union all
SELECT tokens.token_address
FROM {{ source('balancer_v2_optimism','ComposableStablePoolFactory_call_create')}} call_create
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
    FROM {{ source('prices','usd')}}
    {% if is_incremental() %}
    WHERE date_trunc('day', minute) >= date_trunc("day", now() - interval '1 week') and date_trunc('day', minute) < date_trunc('day', now())
    {% else %}
    WHERE date_trunc('day', minute) >= '{{ project_start_date }}' and date_trunc('day', minute) < date_trunc('day', now())
    {% endif %}
    and blockchain = 'optimism'
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
    WHERE date_trunc('day', minute) = date_trunc('day', now())
    and blockchain = 'optimism'
    and contract_address in (select distinct token_address from tokens)
    union all
    SELECT distinct
        DATE_TRUNC('day', minute) AS time,
        lower('0xEdcfaF390906a8f91fb35B7bAC23f3111dBaEe1C') as token,
        'bb-rf-soUSDC',
        18,
        avg(price) AS price
    FROM {{ source('prices','usd')}}
    {% if is_incremental() %}
    WHERE date_trunc('day', minute) >= date_trunc("day", now() - interval '1 week') and date_trunc('day', minute) < date_trunc('day', now())
    {% else %}
    WHERE date_trunc('day', minute) >= '{{ project_start_date }}' and date_trunc('day', minute) < date_trunc('day', now())
    {% endif %}
    and blockchain = 'optimism'
    and contract_address = lower('0x7F5c764cBc14f9669B88837ca1490cCa17c31607')
    group by 1,2,3
union all
    SELECT distinct
        DATE_TRUNC('day', minute), 
        lower('0xEdcfaF390906a8f91fb35B7bAC23f3111dBaEe1C') as token,
        'bb-rf-soUSDC',
        18,
        last_value(price) over (partition by DATE_TRUNC('day', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{ source('prices','usd')}}
    WHERE date_trunc('day', minute) = date_trunc('day', now())
    and blockchain = 'optimism'
    and contract_address = lower('0x7F5c764cBc14f9669B88837ca1490cCa17c31607')
union all
SELECT distinct
        DATE_TRUNC('day', minute) AS time,
        lower('0x6af3737f6d58ae8bcb9f2b597125d37244596e59') as token,
        'bb-rf-soWBTC',
        18,
        avg(price) AS price
    FROM {{ source('prices','usd')}}
    {% if is_incremental() %}
    WHERE date_trunc('day', minute) >= date_trunc("day", now() - interval '1 week') and date_trunc('day', minute) < date_trunc('day', now())
    {% else %}
    WHERE date_trunc('day', minute) >= '{{ project_start_date }}' and date_trunc('day', minute) < date_trunc('day', now())
    {% endif %}
    and blockchain = 'optimism'
    and contract_address = lower('0x68f180fcCe6836688e9084f035309E29Bf0A2095')
    group by 1,2,3
union all
    SELECT distinct
        DATE_TRUNC('day', minute), 
        lower('0x6af3737f6d58ae8bcb9f2b597125d37244596e59') as token,
        'bb-rf-soWBTC',
        18,
        last_value(price) over (partition by DATE_TRUNC('day', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{ source('prices','usd')}}
    WHERE date_trunc('day', minute) = date_trunc('day', now())
    and blockchain = 'optimism'
    and contract_address = lower('0x68f180fcCe6836688e9084f035309E29Bf0A2095')
union all
SELECT distinct
        DATE_TRUNC('day', minute) AS time,
        lower('0x7e9250cc13559eb50536859e8c076ef53e275fb3') as token,
        'bb-rf-soWSTETH',
        18,
        avg(price) AS price
    FROM {{ source('prices','usd')}}
    {% if is_incremental() %}
    WHERE date_trunc('day', minute) >= date_trunc("day", now() - interval '1 week') and date_trunc('day', minute) < date_trunc('day', now())
    {% else %}
    WHERE date_trunc('day', minute) >= '{{ project_start_date }}' and date_trunc('day', minute) < date_trunc('day', now())
    {% endif %}
    
    and blockchain = 'optimism'
    and contract_address = lower('0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb')
    group by 1,2,3
union all
    SELECT distinct
        DATE_TRUNC('day', minute), 
        lower('0x7e9250cc13559eb50536859e8c076ef53e275fb3') as token,
        'bb-rf-soWSTETH',
        18,
        last_value(price) over (partition by DATE_TRUNC('day', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{ source('prices','usd')}}
    WHERE date_trunc('day', minute) = date_trunc('day', now())
    and blockchain = 'optimism'
    and contract_address = lower('0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb')

)

, wsteth_prices_hourly AS (
    select time, lead(time,1, DATE_TRUNC('hour', now() + interval '1' hour)) over (order by time) as next_time, price
    from (
    SELECT distinct
        DATE_TRUNC('hour', minute) time,
        last_value(price) over (partition by DATE_TRUNC('hour', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{ source('prices','usd')}}
     {% if is_incremental() %}
    WHERE date_trunc('day', minute) >= date_trunc("day", now() - interval '1 week') 
    {% else %}
    WHERE date_trunc('day', minute) >= '{{ project_start_date }}' 
    {% endif %}and blockchain = 'optimism'
    and contract_address = lower('0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb')
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
                FROM {{source('balancer_v2_optimism','Vault_evt_Swap')}}
                {% if is_incremental() %}
                WHERE date_trunc('day', evt_block_time) >= date_trunc("day", now() - interval '1 week') 
                {% else %}
                WHERE date_trunc('day', evt_block_time) >= '{{ project_start_date }}'
                {% endif %} 
                and poolId in (select pool_id from pools)
                UNION
                ALL
                SELECT
                    date_trunc('day', evt_block_time) AS day,
                    poolId AS pool_id,
                    tokenOut AS token,
                    -amountOut AS delta
                FROM {{source('balancer_v2_optimism','Vault_evt_Swap')}}
                {% if is_incremental() %}
                WHERE date_trunc('day', evt_block_time) >= date_trunc("day", now() - interval '1 week') 
                {% else %}
                WHERE date_trunc('day', evt_block_time) >= '{{ project_start_date }}'
                {% endif %} 
                and poolId in (select pool_id from pools)
            ) swaps
        GROUP BY 1, 2, 3
)
, zipped_balance_changes AS (
        SELECT
            date_trunc('day', evt_block_time) AS day,
            poolId AS pool_id,
            explode(arrays_zip(tokens, deltas, protocolFeeAmounts)) AS zipped
        FROM {{source('balancer_v2_optimism','Vault_evt_PoolBalanceChanged')}}
        {% if is_incremental() %}
        WHERE date_trunc('day', evt_block_time) >= date_trunc("day", now() - interval '1 week') 
        {% else %}
        WHERE date_trunc('day', evt_block_time) >= '{{ project_start_date }}'
        {% endif %} 
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
        FROM {{source('balancer_v2_optimism','Vault_evt_PoolBalanceManaged')}}
        {% if is_incremental() %}
        WHERE date_trunc('day', evt_block_time) >= date_trunc("day", now() - interval '1 week') 
        {% else %}
        WHERE date_trunc('day', evt_block_time) >= '{{ project_start_date }}'
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
            b.pool_id,
            b.token,
            COALESCE(t.symbol, p1.symbol) AS token_symbol,
            cumulative_amount as token_balance_raw,
            cumulative_amount / POWER(10, COALESCE(t.decimals, p1.decimals)) AS token_balance,
            cumulative_amount / POWER(10, COALESCE(t.decimals, p1.decimals)) * COALESCE(p1.price, 0) AS amount_usd, 
            row_number() OVER(PARTITION BY c.day,pool_id ORDER BY  c.day,pool_id) as row_numb
        FROM dates c
        LEFT JOIN cumulative_balance b ON b.day <= c.day AND c.day < b.day_of_next_change
        LEFT JOIN {{source('prices','tokens')}} t ON t.contract_address = b.token AND blockchain = "optimism"
        LEFT JOIN tokens_prices_daily p1 ON p1.time = b.day AND p1.token = b.token
        WHERE b.token != SUBSTRING(b.pool_id, 0, 42)
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
where token in (lower('0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb'), lower('0x7e9250cc13559eb50536859e8c076ef53e275fb3')) ) main
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
where token != lower('0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb') and cast(row_numb as int) = cast(2 as int)) paired1
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
where token != lower('0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb') and cast(row_numb as int) = cast(3 as int)) paired2
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
where token != lower('0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb') and cast(row_numb as int) = cast(4 as int)) paired3
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
where token != lower('0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb') and cast(row_numb as int) = cast(5 as int)) paired4
on main.day = paired4.day and main.pool_id = paired4.pool_id 
)

, trading_volume as (    
    select  date_trunc('day', s.evt_block_time) as time,
        poolId,
        sum(
        case when tokenOut in (lower('0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb'), lower('0x7e9250cc13559eb50536859e8c076ef53e275fb3')) then wsteth_price.price*amountOut/1e18 
             when tokenIn in  (lower('0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb'), lower('0x7e9250cc13559eb50536859e8c076ef53e275fb3')) then  wsteth_price.price*amountIn/1e18 
             else 0 end) as trading_volume
    from {{source('balancer_v2_optimism','Vault_evt_Swap')}} s
    left join wsteth_prices_hourly wsteth_price on date_trunc('hour', s.evt_block_time) >= wsteth_price.time and date_trunc('hour', s.evt_block_time) < wsteth_price.next_time
    where s.poolId in (select pool_id from pools)
    group by 1,2
) 

, all_metrics as (
select  pool_id as pool, 'optimism' as blockchain, 'beethoven_x' as project, pools_fee.fee, day as time, main_token, main_token_symbol, 
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


select CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(blockchain,CONCAT(' ', project)) ,' '), coalesce(paired_token_symbol,'unknown')),':') , main_token_symbol, ' ', fee) as pool_name,* 
from all_metrics

