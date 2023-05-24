{{ config(
    alias = 'borrow',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['Transaction_Hash'],
    post_hook='{{ expose_spells(\'["polygon"]\',
                                "project",
                                "timeswap",
                                \'["raveena15, varunhawk19"]\') }}'
    )
}}


SELECT
    l.call_tx_hash as Transaction_Hash,
    l.call_block_time as Time,
    CAST(get_json_object(l.param, '$.isToken') AS BOOLEAN) AS Token_0,
    get_json_object(l.param, '$.maturity') AS maturity,
    get_json_object(l.param, '$.strike') AS strike,
    i.pool_pair as Pool_Pair,
    i.chain as Chain,
    CAST(
        CASE 
            WHEN CAST(get_json_object(l.param, '$.isToken') AS BOOLEAN) = true 
            THEN CAST(get_json_object(l.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token0_decimals)
            ELSE CAST(get_json_object(l.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token1_decimals) 
        END as DOUBLE
    ) as Token_Amount,
    CAST(
        CASE 
            WHEN CAST(get_json_object(l.param, '$.isToken') AS BOOLEAN) = true 
            THEN CAST(get_json_object(l.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token0_decimals) * p.price
            ELSE CAST(get_json_object(l.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token1_decimals) * p.price
        END as DOUBLE
    ) as USD_Amount
    FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryUniswapV3BorrowGivenPrincipal_call_borrowGivenPrincipal') }} l
    JOIN {{ ref('timeswap_polygon_pools') }} i ON CAST(maturity as VARCHAR(100)) = i.maturity and cast(strike as VARCHAR(100)) = i.strike
    JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', l.call_block_time)
    WHERE p.symbol=i.token0_symbol AND p.blockchain = 'polygon' AND CAST(get_json_object(l.param, '$.isToken') AS BOOLEAN) = true AND l.call_success = true
    {% if is_incremental() %}
    AND l.call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

UNION  

SELECT
    l.call_tx_hash as Transaction_Hash,
    l.call_block_time as Time,
    CAST(get_json_object(l.param, '$.isToken') AS BOOLEAN) AS Token_0,
    get_json_object(l.param, '$.maturity') AS maturity,
    get_json_object(l.param, '$.strike') AS strike,
    i.pool_pair as Pool_Pair,
    i.chain as Chain,
    CAST(
        CASE 
            WHEN CAST(get_json_object(l.param, '$.isToken') AS BOOLEAN) = true 
            THEN CAST(get_json_object(l.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token0_decimals)
            ELSE CAST(get_json_object(l.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token1_decimals) 
        END as DOUBLE
    ) as Token_Amount,
    CAST(
        CASE 
            WHEN CAST(get_json_object(l.param, '$.isToken') AS BOOLEAN) = true 
            THEN CAST(get_json_object(l.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token0_decimals) * p.price
            ELSE CAST(get_json_object(l.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token1_decimals) * p.price
        END as DOUBLE
    ) as USD_Amount
    FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryUniswapV3BorrowGivenPrincipal_call_borrowGivenPrincipal') }} l
    JOIN {{ ref('timeswap_polygon_pools') }} i ON CAST(maturity as VARCHAR(100)) = i.maturity and cast(strike as VARCHAR(100)) = i.strike
    JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', l.call_block_time)
    WHERE p.symbol=i.token0_symbol AND p.blockchain = 'polygon' AND CAST(get_json_object(l.param, '$.isToken') AS BOOLEAN) = false AND l.call_success = true
    {% if is_incremental() %}
    AND l.call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

UNION

SELECT
  l.evt_tx_hash as Transaction_Hash,
  l.evt_block_time as Time,
  l.isToken0 as Token_0,
  l.maturity as maturity,
  l.strike as strike, 
  i.pool_pair as Pool_Pair,
  i.chain as Chain,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS DOUBLE) / power(10,i.token0_decimals)
      ELSE CAST(l.tokenAmount AS DOUBLE) / power(10,i.token1_decimals)
    END as DOUBLE
  ) as Token_Amount,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS DOUBLE) / power(10,i.token0_decimals) * p.price
      ELSE CAST(l.tokenAmount AS DOUBLE) / power(10,i.token1_decimals) * p.price
    END as DOUBLE
  ) as USD_Amount
  FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryUniswapV3BorrowGivenPrincipal_evt_BorrowGivenPrincipal') }} l
  JOIN {{ ref('timeswap_polygon_pools') }} i ON CAST(l.maturity as VARCHAR(100)) = i.maturity and cast(l.strike as VARCHAR(100)) = i.strike
  JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', l.evt_block_time)
  WHERE p.symbol=i.token0_symbol AND p.blockchain = 'polygon' AND l.isToken0 = true
  {% if is_incremental() %}
    AND l.evt_block_time >= date_trunc("day", now() - interval '1 week')
  {% endif %}

UNION
 
SELECT
  l.evt_tx_hash as Transaction_Hash,
  l.evt_block_time as Time,
  l.isToken0 as Token_0,
  l.maturity as maturity,
  l.strike as strike,
  i.pool_pair as Pool_Pair,
  i.chain as Chain,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS DOUBLE) / power(10,i.token0_decimals)
      ELSE CAST(l.tokenAmount AS DOUBLE) / power(10,i.token1_decimals)
    END as DOUBLE
  ) as Token_Amount,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS DOUBLE) / power(10,i.token0_decimals) * p.price
      ELSE CAST(l.tokenAmount AS DOUBLE) / power(10,i.token1_decimals) * p.price
    END as DOUBLE
  ) as USD_Amount
  FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryUniswapV3BorrowGivenPrincipal_evt_BorrowGivenPrincipal') }} l
  JOIN {{ ref('timeswap_polygon_pools') }} i ON CAST(l.maturity as VARCHAR(100)) = i.maturity and cast(l.strike as VARCHAR(100)) = i.strike
  JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', l.evt_block_time)
  WHERE p.symbol=i.token1_symbol AND p.blockchain = 'polygon' AND l.isToken0 = false
  {% if is_incremental() %}
    AND l.evt_block_time >= date_trunc("day", now() - interval '1 week')
  {% endif %}


UNION

SELECT
  l.evt_tx_hash as Transaction_Hash,
  l.evt_block_time as Time,
  l.isToken0 as Token_0,
  l.maturity as maturity,
  l.strike as strike,
  i.pool_pair as Pool_Pair,
  i.chain as Chain,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS DOUBLE) / power(10,i.token0_decimals)
      ELSE CAST(l.tokenAmount AS DOUBLE) / power(10,i.token1_decimals)
    END as DOUBLE
  ) as Token_Amount,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS DOUBLE) / power(10,i.token0_decimals) * p.price
      ELSE CAST(l.tokenAmount AS DOUBLE) / power(10,i.token1_decimals) * p.price
    END as DOUBLE
  ) as USD_Amount
  FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryNoDexBorrowGivenPrincipal_evt_BorrowGivenPrincipal') }} l
  JOIN {{ ref('timeswap_polygon_pools') }} i ON CAST(l.maturity as VARCHAR(100)) = i.maturity and cast(l.strike as VARCHAR(100)) = i.strike
  JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', l.evt_block_time)
  WHERE p.symbol=i.token0_symbol AND p.blockchain = 'polygon' AND l.isToken0 = true
  {% if is_incremental() %}
    AND l.evt_block_time >= date_trunc("day", now() - interval '1 week')
  {% endif %}

UNION
 
SELECT
  l.evt_tx_hash as Transaction_Hash,
  l.evt_block_time as Time,
  l.isToken0 as Token_0,
  l.maturity as maturity,
  l.strike as strike,
  i.pool_pair as Pool_Pair,
  i.chain as Chain,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS DOUBLE) / power(10,i.token0_decimals)
      ELSE CAST(l.tokenAmount AS DOUBLE) / power(10,i.token1_decimals)
    END as DOUBLE
  ) as Token_Amount,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS DOUBLE) / power(10,i.token0_decimals) * p.price
      ELSE CAST(l.tokenAmount AS DOUBLE) / power(10,i.token1_decimals) * p.price
    END as DOUBLE
  ) as USD_Amount
  FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryNoDexBorrowGivenPrincipal_evt_BorrowGivenPrincipal') }} l
  JOIN {{ ref('timeswap_polygon_pools') }} i ON CAST(l.maturity as VARCHAR(100)) = i.maturity and cast(l.strike as VARCHAR(100)) = i.strike
  JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', l.evt_block_time)
  WHERE p.symbol=i.token0_symbol AND p.blockchain = 'polygon' AND l.isToken0 = false
  {% if is_incremental() %}
    AND l.evt_block_time >= date_trunc("day", now() - interval '1 week')
  {% endif %}



