{{ config(
    schema='lido_liquidity_arbitrum',
    alias = alias('wombat_pools'),
    partition_by = ['time'],
    tags = ['dunesql'],
    materialized = 'table',
    file_format = 'delta',
    unique_key = ['pool', 'time'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "lido_liquidity",
                                \'["ppclunghe"]\') }}'
    )
}}

{% set project_start_date =  '2023-05-25'%} 

with dates as (
    with day_seq as (select (sequence(cast('{{ project_start_date }}' as date), cast(now() as date), interval '1' day)) as day)
select days.day
from day_seq
cross join unnest(day) as days(day)
  )

, tokens_prices_daily AS (
    SELECT DISTINCT
      DATE_TRUNC('day', minute) AS time,
      contract_address as token,
      AVG(price) AS price
    FROM {{source('prices','usd')}}
    WHERE
      DATE_TRUNC('day', minute) >= DATE '{{ project_start_date }}'
      AND DATE_TRUNC('day', minute) < DATE_TRUNC('day', now())
      AND blockchain = 'arbitrum'
      AND contract_address = 0x5979d7b546e38e414f7e9822514be443a4800529
    GROUP BY 1,2
    UNION ALL
    SELECT DISTINCT 
      DATE_TRUNC('day', minute),
      contract_address AS token,
      LAST_VALUE(price) OVER (PARTITION BY  DATE_TRUNC('day', minute), contract_address  ORDER BY minute NULLS FIRST range BETWEEN UNBOUNDED preceding AND UNBOUNDED following) AS price
    FROM {{source('prices','usd')}}
    WHERE
      DATE_TRUNC('day', minute) = DATE_TRUNC('day', now())
      AND blockchain = 'arbitrum'
      AND contract_address = 0x5979d7b546e38e414f7e9822514be443a4800529
  )

, wsteth_prices_hourly AS (
SELECT
      time,
      LEAD(time,1,DATE_TRUNC('hour', now() + INTERVAL '1' hour)) OVER (ORDER BY time NULLS FIRST) AS next_time,
      price
    FROM
      (
    SELECT DISTINCT
      DATE_TRUNC('hour', minute) AS time,
      AVG(price) AS price
    FROM {{source('prices','usd')}}
    WHERE
      DATE_TRUNC('day', minute) >= DATE '{{ project_start_date }}'
      AND DATE_TRUNC('day', minute) < DATE_TRUNC('day', now())
      AND blockchain = 'arbitrum'
      AND contract_address = 0x5979d7b546e38e414f7e9822514be443a4800529
    GROUP BY 1
  
  ))

, swap_events AS (
    SELECT
      DATE_TRUNC('day', sw.evt_block_time) AS time,
      sw.contract_address AS pool,
      SUM(case when fromToken = 0x5979d7b546e38e414f7e9822514be443a4800529 then cast(fromAmount as double) else -cast(toAmount AS DOUBLE) end) AS amount0,
      SUM(case when fromToken = 0x5979d7b546e38e414f7e9822514be443a4800529 then -cast(toAmount as double) else cast(fromAmount AS DOUBLE) end) AS amount1
    FROM {{source('wombat_arbitrum','wsteth_pool_evt_Swap')}} AS sw
    WHERE DATE_TRUNC('day', sw.evt_block_time) >= DATE '{{ project_start_date }}'
    GROUP BY  1,2
 )
 
  , deposit_wsteth_events AS (
    SELECT
      DATE_TRUNC('day', sw.evt_block_time) AS time,
      sw.contract_address AS pool,
      SUM(cast(amount as double)) AS amount0
    FROM {{source('wombat_arbitrum','wsteth_pool_evt_Deposit')}} AS sw
    WHERE DATE_TRUNC('day', sw.evt_block_time) >= DATE '{{ project_start_date }}'
    and token = 0x5979d7b546e38e414f7e9822514be443a4800529
    GROUP BY  1,2
    
 )

 , withdraw_wsteth_events AS (
    SELECT
      DATE_TRUNC('day', sw.evt_block_time) AS time,
      sw.contract_address AS pool,
      SUM(cast(amount as double)) AS amount0
    FROM {{source('wombat_arbitrum','wsteth_pool_evt_Withdraw')}} AS sw
    WHERE DATE_TRUNC('day', sw.evt_block_time) >= DATE '{{ project_start_date }}'
    and token = 0x5979d7b546e38e414f7e9822514be443a4800529
    GROUP BY  1,2
    
 )

 , daily_delta_balance as (
select time, pool, token0, sum(amount0) as amount0
from (
select time, pool, 0x5979d7b546e38e414f7e9822514be443a4800529 as token0, amount0
from swap_events
union all
select time, pool, 0x5979d7b546e38e414f7e9822514be443a4800529 as token0, amount0
from deposit_wsteth_events
union all
select time, pool, 0x5979d7b546e38e414f7e9822514be443a4800529 as token0, -amount0
from withdraw_wsteth_events
)
group by 1,2,3
)


, pool_liquidity AS (
    SELECT
      time,
      LEAD(time, 1, now() + INTERVAL '1' day) OVER (ORDER BY time NULLS FIRST ) AS next_time,
      pool,
      token0,
      SUM(amount0) OVER (PARTITION BY pool  ORDER BY time NULLS FIRST) AS amount0
    FROM
      daily_delta_balance
)

, swap_events_hourly AS (
      SELECT
          DATE_TRUNC('hour', sw.evt_block_time) as hour,
          sw.contract_address AS pool,
          SUM(case when fromToken = 0x5979d7b546e38e414f7e9822514be443a4800529 then cast(fromAmount as double) else cast(toAmount AS DOUBLE) end) AS amount0
        FROM {{source('wombat_arbitrum','wsteth_pool_evt_Swap')}} AS sw 
        WHERE DATE_TRUNC('day', sw.evt_block_time) >= DATE '{{ project_start_date }}'
        GROUP BY 1,2
        
  )
  
, trading_volume_hourly AS (
    SELECT
      hour AS time,
      pool,
      amount0/1e18 as amount,
      p.price,
      COALESCE((p.price * amount0) / CAST(POWER(10, 18) AS DOUBLE), 0) AS volume
    FROM
      swap_events_hourly AS s
      LEFT JOIN wsteth_prices_hourly AS p ON s.hour >= p.time
      AND s.hour < p.next_time

  )
  
  , trading_volume AS (
    SELECT DISTINCT
      DATE_TRUNC('day', time) AS time,
      pool,
      SUM(volume) AS volume
    FROM
      trading_volume_hourly
    GROUP BY 1,2
  )
  
  select 'arbitrum wombat wstETH one-sided' as pool_name, 0xe14302040c0a1eb6fb5a4a79efa46d60029358d9 as pool,
  'arbitrum' as blockchain, 'wombat' as project, 0.01 as fee, d.day as time, 
  0x5979d7b546e38e414f7e9822514be443a4800529 as main_token, 'wstETH' as main_token_symbol, 
  cast(null as varbinary) as paired_token, '' as paired_token_symbol,
  l.amount0/1e18 as main_token_reserve, 0 as paired_token_reserve,
  p0.price*l.amount0/1e18 as main_token_usd_reserve, 0 as paired_token_usd_reserve,
  coalesce(tv.volume,0)/2 as trading_volume
  FROM dates d 
      LEFT JOIN pool_liquidity AS l on d.day >= DATE_TRUNC('day', l.time) and  d.day <  DATE_TRUNC('day', l.next_time)
      LEFT JOIN tokens_prices_daily AS p0 ON d.day = p0.time
      LEFT JOIN trading_volume AS tv ON d.day = tv.time



 
 

