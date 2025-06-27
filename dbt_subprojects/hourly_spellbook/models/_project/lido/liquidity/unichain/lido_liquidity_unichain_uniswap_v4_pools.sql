{{ config(
    schema='lido_liquidity_unichain',
    alias = 'uniswap_v4_pools',     
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool', 'time'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.time')],
    post_hook='{{ expose_spells(blockchains = \'["unichain"]\',
                                spell_type = "project",
                                spell_name = "lido_liquidity",
                                contributors = \'["pipistrella"]\') }}'
    )
}}

{% set project_start_date = '2025-04-01' %} 

with 
  
  pools AS (
    SELECT  id as pool_id, 
            'unichain' as blockchain,
            'uniswap_v4' AS project,
            currency0 as token0, 
            currency1 as token1,
            cast(fee as double)/10000 as fee
    FROM {{source('uniswap_v4_unichain','PoolManager_evt_Initialize')}}
    WHERE currency0 = 0xc02fE7317D4eb8753a02c35fe019786854A92001 or currency1 = 0xc02fE7317D4eb8753a02c35fe019786854A92001  
  )
  

  , tokens AS (
    SELECT DISTINCT
      token AS address
    FROM
      (
        SELECT token0 AS token
        FROM pools
        UNION
        SELECT token1
        FROM pools
        UNION 
        SELECT 0x4200000000000000000000000000000000000006
      ) AS t  
  )
  
  , tokens_prices_daily AS (
    SELECT DISTINCT
      DATE_TRUNC('day', minute) AS time,
      if(contract_address = 0x4200000000000000000000000000000000000006, 0x0000000000000000000000000000000000000000, contract_address)  AS token,
      decimals, 
      if(symbol = 'WETH', 'ETH', symbol) as symbol,
      AVG(price) AS price
    FROM {{source('prices','usd')}}

    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', minute) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE {{ incremental_predicate('minute') }}
    {% endif %}
      AND DATE_TRUNC('day', minute) < current_date
      AND blockchain = 'unichain'
      AND contract_address IN (SELECT address  FROM tokens)
    GROUP BY 1, 2,3,4

    UNION ALL

    SELECT DISTINCT
      DATE_TRUNC('day', minute),
      if(contract_address = 0x4200000000000000000000000000000000000006, 0x0000000000000000000000000000000000000000, contract_address)  AS token,
      decimals, 
      if(symbol = 'WETH', 'ETH', symbol) as symbol,
      LAST_VALUE(price) OVER (PARTITION BY DATE_TRUNC('day', minute), contract_address  ORDER BY  minute NULLS FIRST range BETWEEN UNBOUNDED preceding  AND UNBOUNDED following) AS price
    FROM {{source('prices','usd')}}
    WHERE
      DATE_TRUNC('day', minute) = current_date
      AND blockchain = 'unichain'
      AND contract_address IN (SELECT address  FROM tokens)  
  )
  
, tokens_prices_hourly AS (
    SELECT DISTINCT
      DATE_TRUNC('hour', minute) AS time,
      LEAD(DATE_TRUNC('hour', minute),1,DATE_TRUNC('hour', NOW() + INTERVAL '1' hour)) OVER (PARTITION BY contract_address  ORDER BY DATE_TRUNC('hour', minute) NULLS FIRST) AS next_time,
      if(contract_address = 0x4200000000000000000000000000000000000006, 0x0000000000000000000000000000000000000000, contract_address)  AS token,
      decimals, 
      if(symbol = 'WETH', 'ETH', symbol) as symbol,
      LAST_VALUE(price) OVER (PARTITION BY DATE_TRUNC('hour', minute), contract_address ORDER BY minute NULLS FIRST range BETWEEN UNBOUNDED preceding AND UNBOUNDED following) AS price
    FROM {{source('prices','usd')}}
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', minute) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE {{ incremental_predicate('minute') }}
    {% endif %}
        AND blockchain = 'unichain'
        AND contract_address IN (SELECT address FROM tokens)      
  )
  
  , get_recent_sqrtPriceX96 AS (
    SELECT *
    FROM (
        SELECT 
            ml.*,
            i.currency0 as token0,
            i.currency1 as token1,
            COALESCE(s.evt_block_time, i.evt_block_time) as most_recent_time,
            COALESCE(s.sqrtPriceX96, i.sqrtPriceX96) AS sqrtPriceX96,
            ROW_NUMBER() OVER (PARTITION BY ml.id, ml.evt_block_time, ml.evt_index ORDER BY CASE WHEN s.sqrtPriceX96 IS NOT NULL THEN s.evt_block_time ELSE i.evt_block_time END DESC) AS rn
        FROM {{source('uniswap_v4_unichain','PoolManager_evt_ModifyLiquidity')}} ml
        JOIN pools ON ml.id = pools.pool_id
        LEFT JOIN {{source('uniswap_v4_unichain','PoolManager_evt_Swap')}} s  ON ml.evt_block_time > s.evt_block_time AND ml.id = s.id
        LEFT JOIN {{source('uniswap_v4_unichain','PoolManager_evt_Initialize')}} i ON ml.evt_block_time >= i.evt_block_time AND i.id = ml.id
    )tbl
    WHERE rn = 1
  )
  , prep_for_calculations AS (
    SELECT evt_block_time,
        evt_block_number,
        id, 
        evt_tx_hash,
        evt_index,
        salt,
        token0,
        token1,
        LOG(sqrtPriceX96/POWER(2, 96), 10)/LOG(1.0001, 10) as tickCurrent,
        tickLower,
        tickUpper,
        SQRT(POWER(1.0001, tickLower)) as sqrtRatioL,
        SQRT(POWER(1.0001, tickUpper)) sqrtRatioU,
        sqrtPriceX96/ POWER(2, 96) sqrtPrice,
        sqrtPriceX96,
        liquidityDelta
    FROM get_recent_sqrtPriceX96
  )

  , base_liquidity_amounts AS (
    SELECT
        evt_block_time,
        evt_block_number,
        id, 
        evt_tx_hash,
        evt_index,
        salt,
        token0,
        token1,
        CASE WHEN sqrtPrice <= sqrtRatioL THEN liquidityDelta * ((sqrtRatioU - sqrtRatioL)/(sqrtRatioL*sqrtRatioU))
        WHEN sqrtPrice >= sqrtRatioU THEN 0
        ELSE liquidityDelta * ((sqrtRatioU - sqrtPrice)/(sqrtPrice*sqrtRatioU))
        END as amount0,
        CASE WHEN sqrtPrice <= sqrtRatioL THEN 0
        WHEN sqrtPrice >= sqrtRatioU THEN liquidityDelta*(sqrtRatioU - sqrtRatioL)
        ELSE liquidityDelta*(sqrtPrice - sqrtRatioL)
        END as amount1
    FROM prep_for_calculations pc
  )

  , liquidity_change_base as (
    SELECT b.id as pool
        , date_trunc('minute', b.evt_block_time) as minute
        , b.evt_tx_hash
        , b.evt_index
        , b.token0
        , b.token1
        , b.amount0
        , b.amount1
    FROM base_liquidity_amounts b
    
    union all -- add liquidty modification together with swaps as they both impact total liquidity
    
    select s.id as pool
        , date_trunc('minute', s.evt_block_time) as minute
        , s.evt_tx_hash
        , s.evt_index
        , p.token0
        , p.token1
        -- v4 signage is from user's perspective, so multiply -1 to flip signage to be from pool's perspective
        , -1* s.amount0
        , -1* s.amount1
    from {{source('uniswap_v4_unichain','PoolManager_evt_Swap')}} s
    join pools p on p.pool_id = s.id 
  )

  , pools_liquidity AS (
    SELECT date_trunc('day', minute) as time, 
           pool,
           token0,
           token1,
           sum(amount0) as amount0,
           sum(amount1) as amount1
    FROM liquidity_change_base   
    GROUP BY 1, 2, 3, 4
    
  )

  , swap_events_hourly AS (
    SELECT sw.evt_block_time as time,
           sw.id AS pool,
           pools.token0,
           pools.token1,
           COALESCE(SUM(CAST(ABS(amount0) AS DOUBLE)), 0) AS amount0,
           COALESCE(SUM(CAST(ABS(amount1) AS DOUBLE)), 0) AS amount1
    FROM {{source('uniswap_v4_unichain','PoolManager_evt_Swap')}} sw 
    INNER JOIN pools on sw.id = pools.pool_id
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', sw.evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE {{ incremental_predicate('sw.evt_block_time') }}
    {% endif %} 
    
    GROUP BY 1, 2, 3, 4
        
  )
  
  
  , trading_volume AS (
    SELECT date_trunc('day', s.time)  AS time,
           pool,
           sum(case when p0.decimals is not null then COALESCE((p0.price * amount0) / CAST(POWER(10, p0.decimals) AS DOUBLE),0)
                    else COALESCE((p1.price * amount1) / CAST(POWER(10, p1.decimals) AS DOUBLE),0)
                    end) AS volume
    FROM
      swap_events_hourly AS s
      LEFT JOIN tokens_prices_hourly AS p0 ON date_trunc('hour', s.time) >= p0.time AND date_trunc('hour', s.time) < p0.next_time  AND s.token0 = p0.token
      LEFT JOIN tokens_prices_hourly AS p1 ON date_trunc('hour', s.time) >= p1.time AND date_trunc('hour', s.time) < p1.next_time  AND s.token1 = p1.token
    group by 1,2  
  )

  , all_metrics AS (
   
    SELECT
      l.pool,
      pools.blockchain,
      pools.project,
      pools.fee,
      cast(l.time as date) as time,
      CASE
        WHEN l.token0 = 0xc02fe7317d4eb8753a02c35fe019786854a92001 THEN l.token0
        ELSE l.token1
      END AS main_token,
      CASE
        WHEN l.token0 = 0xc02fe7317d4eb8753a02c35fe019786854a92001 THEN p0.symbol
        ELSE p1.symbol
      END AS main_token_symbol,
      CASE
        WHEN l.token0 = 0xc02fe7317d4eb8753a02c35fe019786854a92001 THEN l.token1
        ELSE l.token0
      END AS paired_token,
      CASE
        WHEN l.token0 = 0xc02fe7317d4eb8753a02c35fe019786854a92001 THEN p1.symbol
        ELSE p0.symbol
      END AS paired_token_symbol,
      CASE
        WHEN l.token0 = 0xc02fe7317d4eb8753a02c35fe019786854a92001 THEN amount0 / CAST(POWER(10, p0.decimals) as double)
        ELSE amount1 / CAST(POWER(10, p1.decimals) AS DOUBLE)
      END AS main_token_reserve,
      CASE
        WHEN l.token0 = 0xc02fe7317d4eb8753a02c35fe019786854a92001 THEN amount1 / CAST(POWER(10, p1.decimals) as double)
        ELSE amount0 / CAST(POWER(10, p0.decimals) AS DOUBLE)
      END AS paired_token_reserve,
      CASE
        WHEN l.token0 = 0xc02fe7317d4eb8753a02c35fe019786854a92001 THEN p0.price
        ELSE p1.price
      END AS main_token_usd_price,
      CASE
        WHEN l.token0 = 0xc02fe7317d4eb8753a02c35fe019786854a92001 THEN p1.price
        ELSE p0.price 
      END AS paired_token_usd_price,
      tv.volume AS trading_volume
    FROM
      pools_liquidity AS l
      LEFT JOIN pools ON l.pool = pools.pool_id
      LEFT JOIN tokens AS t0 ON l.token0 = t0.address
      LEFT JOIN tokens AS t1 ON l.token1 = t1.address
      LEFT JOIN tokens_prices_daily AS p0 ON l.time = p0.time   AND l.token0 = p0.token
      LEFT JOIN tokens_prices_daily AS p1 ON l.time = p1.time   AND l.token1 = p1.token
      LEFT JOIN trading_volume AS tv ON l.time = tv.time AND l.pool = tv.pool
      
  )
SELECT blockchain||' '||project||' '||COALESCE(paired_token_symbol, 'unknown')||':'||COALESCE(main_token_symbol, 'unknown')||' '||format('%,.3f',round(coalesce(fee,0),4)) AS pool_name,
  *
FROM
  all_metrics
WHERE main_token_usd_price is not null


