{{ config(
    schema='lido_liquidity_optimism',
    alias = alias('velodrome_pools'),
    tags = ['dunesql'], 
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

{% set project_start_date = '2022-09-14' %}



with 

 pools as (
select pair AS address,
      'optimism' AS blockchain,
      'velodrome' AS project,
      case when stable then 'stable' else 'volatile' end AS pool_type,
      0.02 as fee
from {{source('velodrome_optimism','PairFactory_evt_PairCreated')}}
where token0 = 0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb
      OR token1 = 0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb
)      

, tokens as (
 select distinct token
 from (
 select token0 as token
 from {{source('velodrome_optimism','PairFactory_evt_PairCreated')}}
 where token1 = 0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb
 union all
 select token1
 from {{source('velodrome_optimism','PairFactory_evt_PairCreated')}}
 where token0 = 0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb
 union all
 select 0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb
 ) t
 )
 
 
 , tokens_prices_daily as (     
select distinct 
      DATE_TRUNC('day', minute) AS time,
      contract_address AS token,
      symbol,
      decimals,
      AVG(price) AS price
FROM {{source('prices','usd')}} p
{% if not is_incremental() %}
WHERE DATE_TRUNC('day', p.minute) >= DATE '{{ project_start_date }}'
{% else %}
WHERE DATE_TRUNC('day', p.minute) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
{% endif %}

     and date_trunc('day', minute) < current_date
     and blockchain = 'optimism'
  and contract_address IN (select token from tokens)
group by 1,2,3,4
union all
select distinct
      DATE_TRUNC('day', minute),
      contract_address AS token,
      symbol,
      decimals,      
      LAST_VALUE(price) OVER (PARTITION BY DATE_TRUNC('day', minute),contract_address  ORDER BY minute NULLS FIRST range BETWEEN UNBOUNDED preceding AND UNBOUNDED following) AS price
    FROM
      {{source('prices','usd')}}
    WHERE
      DATE_TRUNC('day', minute) = current_date
      and blockchain = 'optimism'
  and contract_address IN (select token from tokens) 
 )
 
 , wsteth_prices_hourly AS (
    SELECT distinct
        DATE_TRUNC('hour', minute) time, 
        contract_address as token,
        symbol,
        decimals,
        last_value(price) over (partition by DATE_TRUNC('hour', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{ source('prices', 'usd') }} p
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}

      and blockchain = 'optimism' and contract_address = 0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb
    
)

, wsteth_prices_hourly_with_lead AS (
select time, 
       lead(time, 1, date_trunc('hour', now() + interval '1'  hour)) over (order by time) as next_time, 
       price
from wsteth_prices_hourly
)


, mint_events AS (
 select DATE_TRUNC('day', m.evt_block_time) AS time,
      m.contract_address AS pool,
      cr.token0,
      cr.token1,
      SUM(CAST(amount0 AS DOUBLE)) AS amount0,
      SUM(CAST(amount1 AS DOUBLE)) AS amount1
 from {{source('velodrome_optimism','Pair_evt_Mint')}} m
 left join {{source('velodrome_optimism','PairFactory_evt_PairCreated')}} cr on m.contract_address = cr.pair 
 
 {% if not is_incremental() %}
 WHERE DATE_TRUNC('day', m.evt_block_time) >= DATE '{{ project_start_date }}'
 {% else %}
 WHERE DATE_TRUNC('day', m.evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
 {% endif %}
 
 and m.contract_address in (select address from pools)
 group by 1,2,3,4
 )
 
 , burn_events as (
 select DATE_TRUNC('day', b.evt_block_time) AS time,
      b.contract_address AS pool,
      cr.token0,
      cr.token1,
      (-1)*SUM(CAST(amount0 AS DOUBLE)) AS amount0,
      (-1)*SUM(CAST(amount1 AS DOUBLE)) AS amount1
 from {{source('velodrome_optimism','Pair_evt_Burn')}} b
 left join {{source('velodrome_optimism','PairFactory_evt_PairCreated')}} cr on b.contract_address = cr.pair 
 
 {% if not is_incremental() %}
 WHERE DATE_TRUNC('day', b.evt_block_time) >= DATE '{{ project_start_date }}'
 {% else %}
 WHERE DATE_TRUNC('day', b.evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
 {% endif %}
 
 and b.contract_address in (select address from pools)
 group by 1,2,3,4
 
 )

, swap_events as (
 select DATE_TRUNC('day', s.evt_block_time) AS time,
      s.contract_address AS pool,
      cr.token0,
      cr.token1,
      SUM(CAST(amount0In AS DOUBLE) - CAST(amount0Out AS DOUBLE)) AS amount0,
      SUM(CAST(amount1In AS DOUBLE) - CAST(amount1Out AS DOUBLE)) AS amount1
 from {{source('velodrome_optimism','Pair_evt_Swap')}} s
 left join {{source('velodrome_optimism','PairFactory_evt_PairCreated')}} cr on s.contract_address = cr.pair 
 
 {% if not is_incremental() %}
  WHERE DATE_TRUNC('day', s.evt_block_time) >= DATE '{{ project_start_date }}'
 {% else %}
  WHERE DATE_TRUNC('day', s.evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
 {% endif %}
 
 and s.contract_address in (select address from pools)
 group by 1,2,3,4
 
)

, fee_events as (
select
    DATE_TRUNC('day', s.evt_block_time) AS time,
      s.contract_address AS pool,
      cr.token0,
      cr.token1,
      (-1)*SUM(CAST(amount0 AS DOUBLE) ) AS amount0,
      (-1)*SUM(CAST(amount1 AS DOUBLE) ) AS amount1 
 from {{source('velodrome_optimism','Pair_evt_Fees')}} s
 left join {{source('velodrome_optimism','PairFactory_evt_PairCreated')}} cr on s.contract_address = cr.pair 
 
 {% if not is_incremental() %}
 WHERE DATE_TRUNC('day', s.evt_block_time) >= DATE '{{ project_start_date }}'
 {% else %}
 WHERE DATE_TRUNC('day', s.evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
 {% endif %}
 and s.contract_address in (select address from pools)
 group by 1,2,3,4
 )
 
, daily_delta_balance AS (

select time, pool, token0, token1, sum(amount0) as amount0, sum(amount1) as amount1
from (
select time, pool,token0, token1, amount0, amount1
from  mint_events

union all

select time, pool,token0, token1, amount0, amount1
from  swap_events

union all

select time, pool,token0, token1, amount0, amount1
from  burn_events

union all

select time, pool,token0, token1, amount0, amount1
from  fee_events
) group by 1,2,3,4
)

, daily_delta_balance_with_lead AS (
select time, pool, token0, token1, amount0, amount1, 
lead(time, 1, now()) over (partition by pool order by time) as next_time
from daily_delta_balance
)


, pool_liquidity as (
SELECT  b.time,
        b.pool,
        token0,
        token1,
        SUM(amount0) AS amount0,
        SUM(amount1) AS amount1
FROM daily_delta_balance_with_lead b
GROUP BY 1,2,3,4
)

, swap_events_hourly as (
 select DATE_TRUNC('hour', s.evt_block_time) AS time,
      s.contract_address AS pool,
      sum(case when cr.token0 = 0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb then CAST(amount0In AS DOUBLE) + CAST(amount0Out AS DOUBLE)
      else CAST(amount1In AS DOUBLE) + CAST(amount1Out AS DOUBLE) end) as wsteth_amount
 from {{source('velodrome_optimism','Pair_evt_Swap')}} s
 left join {{source('velodrome_optimism','PairFactory_evt_PairCreated')}} cr on s.contract_address = cr.pair 

 {% if not is_incremental() %}
 WHERE DATE_TRUNC('day', s.evt_block_time) >= DATE '{{ project_start_date }}'
 {% else %}
 WHERE DATE_TRUNC('day', s.evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
 {% endif %}

 and s.contract_address in (select address from pools)
group by 1,2
)

, trading_volume_hourly AS (
    SELECT
      s.time,
      pool,
      wsteth_amount,
      p.price,
      COALESCE((p.price * wsteth_amount) / CAST(POWER(10, 18) AS DOUBLE), 0) AS volume
    FROM
      swap_events_hourly AS s
      LEFT JOIN wsteth_prices_hourly_with_lead AS p ON s.time >= p.time
      AND s.time < p.next_time
  )
  
, trading_volume AS (
    SELECT DISTINCT
      DATE_TRUNC('day', time) AS time,
      pool,
      SUM(volume) AS volume
    FROM trading_volume_hourly
    GROUP BY 1, 2
    )
  
, all_metrics AS (
    SELECT
      l.pool,
      pools.blockchain,
      pools.project,
      pools.fee,
      pools.pool_type,
      cast(l.time as date) as time,
      CASE WHEN token0 = 0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb THEN token0 ELSE token1 END AS main_token,
      CASE WHEN token0 = 0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb THEN p0.symbol ELSE p1.symbol END AS main_token_symbol,
      CASE WHEN token0 = 0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb THEN token1 ELSE token0 END AS paired_token,
      CASE WHEN token0 = 0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb THEN p1.symbol ELSE p0.symbol END AS paired_token_symbol,
      CASE WHEN token0 = 0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb THEN  amount0/CAST(POWER(10,p0.decimals) as double) ELSE amount1/CAST(POWER(10,p1.decimals) as double) END AS main_token_reserve,
      CASE WHEN token0 = 0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb THEN amount1/CAST(POWER(10, p1.decimals) as double) ELSE amount0/CAST(POWER(10, p0.decimals) as double) END AS paired_token_reserve,
      CASE WHEN token0 = 0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb THEN p0.price ELSE p1.price END AS main_token_usd_price,
      CASE WHEN token0 = 0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb THEN p1.price ELSE p0.price END AS paired_token_usd_price,
      coalesce(volume,0) AS trading_volume
    FROM
      pool_liquidity AS l
      LEFT JOIN pools ON l.pool = pools.address
      LEFT JOIN tokens AS t0 ON l.token0 = t0.token
      LEFT JOIN tokens AS t1 ON l.token1 = t1.token
      LEFT JOIN tokens_prices_daily AS p0 ON l.time = p0.time
      AND l.token0 = p0.token
      LEFT JOIN tokens_prices_daily AS p1 ON l.time = p1.time
      AND l.token1 = p1.token
      LEFT JOIN trading_volume AS tv ON l.time = tv.time
      AND l.pool = tv.pool
 )
 
select  CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(blockchain,CONCAT(' ', project)) ,' '), coalesce(paired_token_symbol,'unknown')),':') , main_token_symbol, ' ', pool_type) as pool_name,
        pool,
        blockchain,
        project,
        fee,
        time,
        main_token,
        main_token_symbol,
        paired_token,
        paired_token_symbol,
        main_token_reserve,
        paired_token_reserve,
        main_token_usd_price,
        paired_token_usd_price,
        trading_volume
from all_metrics
