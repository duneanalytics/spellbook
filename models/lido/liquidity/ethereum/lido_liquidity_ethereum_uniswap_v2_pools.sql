{{ config(
    alias = 'uniswap_v2_pools',
                 
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool', 'time'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido_liquidity",
                                \'["ppclunghe", "gregshestakovlido"]\') }}'
    )
}}

{% set project_start_date = '2020-12-19' %} 

with dates as (
    with day_seq as (select (sequence(cast('{{ project_start_date }}' as date), current_date, interval '1' day)) as day)
select days.day
from day_seq
cross join unnest(day) as days(day)
)
 
 
 , pools AS (
    SELECT
      pair AS address,
      'ethereum' AS blockchain,
      'uniswap_v2' AS project,
      0.003 as fee, 
      token0, token1
    FROM
      {{source('uniswap_v2_ethereum','Factory_evt_PairCreated')}}
    WHERE pair = 0x4028daac072e492d34a3afdbef0ba7e35d8b55c4
  )
  


  , tokens AS (
    SELECT DISTINCT
      token AS address
    FROM
      (
        SELECT token1 AS token
        FROM {{source('uniswap_v2_ethereum','Factory_evt_PairCreated')}}
        WHERE token0 = 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84
         and pair = 0x4028daac072e492d34a3afdbef0ba7e35d8b55c4
        UNION
        SELECT token0
        FROM {{source('uniswap_v2_ethereum','Factory_evt_PairCreated')}}
        WHERE token1 = 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84
        and pair = 0x4028daac072e492d34a3afdbef0ba7e35d8b55c4
        UNION
        SELECT  0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84
      ) AS t
      
  )
  
  , tokens_prices_daily AS (
    SELECT DISTINCT
      DATE_TRUNC('day', minute) AS time,
      contract_address  AS token,
      decimals, 
      symbol,
      AVG(price) AS price
    FROM {{source('prices','usd')}} p
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}
      AND DATE_TRUNC('day', minute) < current_date
      AND blockchain = 'ethereum'
      AND contract_address IN (SELECT address  FROM tokens      )
    GROUP BY 1, 2,3,4
    UNION ALL
    SELECT DISTINCT
      DATE_TRUNC('day', minute),
      contract_address  AS token,
      decimals, 
      symbol,
      LAST_VALUE(price) OVER (
        PARTITION BY
          DATE_TRUNC('day', minute),
          contract_address
        ORDER BY
          minute NULLS FIRST range BETWEEN UNBOUNDED preceding
          AND UNBOUNDED following
      ) AS price
    FROM {{source('prices','usd')}} p
    WHERE
      DATE_TRUNC('day', minute) = current_date
      AND blockchain = 'ethereum'
      AND contract_address IN (SELECT address  FROM tokens      )
  )
  
  
  , tokens_prices_hourly AS (
        SELECT DISTINCT
          DATE_TRUNC('hour', minute) AS time,
          LEAD(DATE_TRUNC('hour', minute),1,DATE_TRUNC('hour', NOW() + INTERVAL '1' hour)) OVER (PARTITION BY contract_address  ORDER BY DATE_TRUNC('hour', minute) NULLS FIRST) AS next_time,
          contract_address  AS token,
          decimals, 
          symbol,
          LAST_VALUE(price) OVER (
            PARTITION BY
              DATE_TRUNC('hour', minute),
              contract_address
            ORDER BY
              minute NULLS FIRST range BETWEEN UNBOUNDED preceding
              AND UNBOUNDED following
          ) AS price
        FROM {{source('prices','usd')}} p
        {% if not is_incremental() %}
        WHERE DATE_TRUNC('day', p.minute) >= DATE '{{ project_start_date }}'
        {% else %}
        WHERE DATE_TRUNC('day', p.minute) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
        {% endif %}
        AND blockchain = 'ethereum'
        AND contract_address IN (SELECT address FROM tokens)
      
  )
  
  , swap_events AS (
    SELECT
      DATE_TRUNC('day', sw.evt_block_time) AS time,
      sw.contract_address AS pool,
      cr.token0,
      cr.token1,
      SUM(CAST(amount0In AS DOUBLE)-CAST(amount0Out AS DOUBLE)) AS amount0,
      SUM(CAST(amount1In AS DOUBLE)-CAST(amount1Out AS DOUBLE)) AS amount1
    FROM
      {{source('uniswap_v2_ethereum','Pair_evt_Swap')}} as sw
      LEFT JOIN {{source('uniswap_v2_ethereum','Factory_evt_PairCreated')}} AS cr ON sw.contract_address = cr.pair
    
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', sw.evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE DATE_TRUNC('day', sw.evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}
    and sw.contract_address IN (SELECT address  FROM  pools)
    GROUP BY 1,2,3,4
  )
  
  , mint_events AS (
    SELECT
      DATE_TRUNC('day', mt.evt_block_time) AS time,
      mt.contract_address AS pool,
      cr.token0,
      cr.token1,
      SUM(CAST(amount0 AS DOUBLE)) AS amount0,
      SUM(CAST(amount1 AS DOUBLE)) AS amount1
    FROM {{source('uniswap_v2_ethereum','Pair_evt_Mint')}} as mt
      LEFT JOIN {{source('uniswap_v2_ethereum','Factory_evt_PairCreated')}} AS cr ON mt.contract_address = cr.pair
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', mt.evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE DATE_TRUNC('day', mt.evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}
      and mt.contract_address IN (SELECT address FROM pools)
    GROUP BY 1,  2,  3,  4
   
  )

  , burn_events AS (
    SELECT
      DATE_TRUNC('day', bn.evt_block_time) AS time,
      bn.contract_address AS pool,
      cr.token0,
      cr.token1,
      (-1) * SUM(CAST(amount0 AS DOUBLE)) AS amount0,
      (-1) * SUM(CAST(amount1 AS DOUBLE)) AS amount1
    FROM {{source('uniswap_v2_ethereum','Pair_evt_Burn')}} AS bn
      LEFT JOIN {{source('uniswap_v2_ethereum','Factory_evt_PairCreated')}} AS cr ON bn.contract_address = cr.pair
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', bn.evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE DATE_TRUNC('day', bn.evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}
      and bn.contract_address IN (SELECT address FROM pools)
    GROUP BY 1, 2, 3, 4
  )
  
  , daily_delta_balance as (
    select time, pool, token0, token1, sum(coalesce(amount0, 0)) as amount0, sum(coalesce(amount1, 0)) as amount1
    from ( 
    select time, pool, token0, token1, amount0, amount1 
    from swap_events
    union all
    select time, pool, token0, token1, amount0, amount1 
    from mint_events
    union all
    select time, pool, token0, token1, amount0, amount1 
    from burn_events
    ) balance
    group by 1,2,3,4
)
  
, pool_liquidity as (
    select  time, 
            pool, 
            token0, 
            token1, 
            sum(amount0) as amount0, 
            sum(amount1) as amount1
    from daily_delta_balance
    group by 1,2,3,4
)
 
, swap_events_hourly AS (
        SELECT
          sw.evt_block_time as time,
          sw.contract_address AS pool,
          token0,
          token1,
          COALESCE(SUM(CAST(ABS(amount0In+amount0Out) AS DOUBLE)), 0) AS amount0,
          COALESCE(SUM(CAST(ABS(amount1In+amount1Out) AS DOUBLE)), 0) AS amount1
        FROM
          {{source('uniswap_v2_ethereum','Pair_evt_Swap')}} AS sw 
          inner join pools on sw.contract_address = pools.address
        {% if not is_incremental() %}
        WHERE DATE_TRUNC('day', sw.evt_block_time) >= DATE '{{ project_start_date }}'
        {% else %}
        WHERE DATE_TRUNC('day', sw.evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
        {% endif %}         
        GROUP BY 1, 2, 3, 4
        
  )
  
  
 , trading_volume AS (
    SELECT
      date_trunc('day', s.time)  AS time,
      pool,
      sum(COALESCE((p.price * amount0) / CAST(POWER(10, p.decimals) AS DOUBLE),0)) AS volume
    FROM
      swap_events_hourly AS s
      LEFT JOIN tokens_prices_hourly AS p ON date_trunc('hour', s.time) >= p.time
      AND date_trunc('hour', s.time) < p.next_time
      AND s.token0 = p.token
    group by 1,2  
  )
   
 , all_metrics AS (
 
 select l.pool, pools.blockchain, pools.project, pools.fee, cast(l.time as date) as time, 
    case when l.token0 = 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84 then l.token0 else l.token1 end main_token,
    case when l.token0 = 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84 then coalesce(p0.symbol, 'stETH') else coalesce(p1.symbol, 'stETH') end main_token_symbol,
    case when l.token0 = 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84 then l.token1 else l.token0 end paired_token,
    case when l.token0 = 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84 then p1.symbol else p0.symbol end paired_token_symbol, 
    --it's right only for uni v2 stETH:ETH pool
    case when l.token0 = 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84 then amount0/power(10, coalesce(p0.decimals, p1.decimals))  else amount1/power(10, coalesce(p1.decimals, p0.decimals))  end main_token_reserve,
    case when l.token0 = 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84 then amount1/power(10, p1.decimals)  else amount0/power(10, p0.decimals)  end paired_token_reserve,
    --it's right only for uni v2 stETH:ETH pool
    case when l.token0 = 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84 then coalesce(p0.price, p1.price) else coalesce(p1.price, p0.price) end as main_token_usd_price,
    case when l.token0 = 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84 then p1.price else p0.price end as paired_token_usd_price,
    volume as trading_volume
from pool_liquidity l 
left join pools on l.pool = pools.address
left join tokens t0 on l.token0 = t0.address
left join tokens t1 on l.token1 = t1.address
left join tokens_prices_daily p0 on l.time = p0.time and l.token0 = p0.token
left join tokens_prices_daily p1 on l.time = p1.time and l.token1 = p1.token
left join trading_volume AS tv ON l.time = tv.time AND l.pool = tv.pool
  )
  

 
select CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(blockchain,CONCAT(' ', project)) ,' '), COALESCE(paired_token_symbol, 'unknown')),':') , main_token_symbol, ' ', format('%,.3f',round(coalesce(fee,0),4))) as pool_name,* 
from all_metrics
