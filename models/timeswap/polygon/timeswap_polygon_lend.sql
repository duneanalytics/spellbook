{{ config(
    alias = 'lend',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_hash', 'pool_pair', 'maturity', 'strike'],
    post_hook='{{ expose_spells(\'["polygon"]\',
                                "project",
                                "timeswap",
                                \'["raveena15, varunhawk19"]\') }}'
    )
}}


SELECT
    l.call_tx_hash as transaction_hash,
    l.call_block_time as time,
    CAST(get_json_object(l.param, '$.isToken') AS BOOLEAN) AS token_0,
    'lend' as transaction_type,
    get_json_object(l.param, '$.maturity') AS maturity,
    get_json_object(l.param, '$.strike') AS strike,
    i.pool_pair as pool_pair,
    i.chain as chain,
    CAST(
        CASE 
            WHEN CAST(get_json_object(l.param, '$.isToken') AS BOOLEAN) = true 
            THEN CAST(get_json_object(l.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token0_decimals)
            ELSE CAST(get_json_object(l.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token1_decimals) 
        END as DOUBLE
    ) as token_amount,
    CAST(
        CASE 
            WHEN CAST(get_json_object(l.param, '$.isToken') AS BOOLEAN) = true 
            THEN CAST(get_json_object(l.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token0_decimals) * p.price
            ELSE CAST(get_json_object(l.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token1_decimals) * p.price
        END as DOUBLE
    ) as usd_amount
    FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryUniswapV3LendGivenPrincipal_call_lendGivenPrincipal') }} l
    JOIN {{ ref('timeswap_polygon_pools') }} i ON CAST(maturity as VARCHAR(100)) = i.maturity and cast(strike as VARCHAR(100)) = i.strike
    JOIN {{ source('prices', 'usd') }} p 
    ON p.symbol=i.token0_symbol 
    and p.blockchain = 'polygon' 
    and l.call_success = true
    and CAST(get_json_object(l.param, '$.isToken') AS BOOLEAN) = true
    AND p.minute = date_trunc('minute',l.call_block_time)
    {% if is_incremental() %}
    AND p.minute >= date_trunc("day", now() - interval '1 week')
    WHERE l.call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

UNION  

SELECT
    l.call_tx_hash as transaction_hash,
    l.call_block_time as time,
    CAST(get_json_object(l.param, '$.isToken') AS BOOLEAN) AS token_0,
    'lend' as transaction_type,
    get_json_object(l.param, '$.maturity') AS maturity,
    get_json_object(l.param, '$.strike') AS strike,
    i.pool_pair as pool_pair,
    i.chain as chain,
    CAST(
        CASE 
            WHEN CAST(get_json_object(l.param, '$.isToken') AS BOOLEAN) = true 
            THEN CAST(get_json_object(l.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token0_decimals)
            ELSE CAST(get_json_object(l.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token1_decimals) 
        END as DOUBLE
    ) as token_amount,
    CAST(
        CASE 
            WHEN CAST(get_json_object(l.param, '$.isToken') AS BOOLEAN) = true 
            THEN CAST(get_json_object(l.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token0_decimals) * p.price
            ELSE CAST(get_json_object(l.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token1_decimals) * p.price
        END as DOUBLE
    ) as usd_amount
    FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryUniswapV3LendGivenPrincipal_call_lendGivenPrincipal') }} l
    JOIN {{ ref('timeswap_polygon_pools') }} i ON CAST(maturity as VARCHAR(100)) = i.maturity and cast(strike as VARCHAR(100)) = i.strike
    JOIN {{ source('prices', 'usd') }} p 
    ON p.symbol=i.token1_symbol 
    and p.blockchain = 'polygon' 
    and l.call_success = true
    and CAST(get_json_object(l.param, '$.isToken') AS BOOLEAN) = false
    AND p.minute = date_trunc('minute',l.call_block_time)
    {% if is_incremental() %}
    AND p.minute >= date_trunc("day", now() - interval '1 week')
    WHERE l.call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

UNION

SELECT
  l.evt_tx_hash as transaction_hash,
  l.evt_block_time as time,
  l.isToken0 as token_0,
  'lend' as transaction_type,
  l.maturity as maturity,
  l.strike as strike, 
  i.pool_pair as pool_pair,
  i.chain as chain,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS DOUBLE) / power(10,i.token0_decimals)
      ELSE CAST(l.tokenAmount AS DOUBLE) / power(10,i.token1_decimals)
    END as DOUBLE
  ) as token_amount,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS DOUBLE) / power(10,i.token0_decimals) * p.price
      ELSE CAST(l.tokenAmount AS DOUBLE) / power(10,i.token1_decimals) * p.price
    END as DOUBLE
  ) as usd_amount
FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryUniswapV3LendGivenPrincipal_evt_LendGivenPrincipal') }} l
JOIN {{ ref('timeswap_polygon_pools') }} i 
  ON CAST(l.maturity as VARCHAR(100)) = i.maturity 
  and cast(l.strike as VARCHAR(100)) = i.strike
JOIN {{ source('prices', 'usd') }} p 
  ON p.symbol=i.token0_symbol 
  and p.blockchain = 'polygon' 
  and l.isToken0 = true
  AND p.minute = date_trunc('minute',l.evt_block_time)
  {% if is_incremental() %}
  AND p.minute >= date_trunc("day", now() - interval '1 week')
WHERE l.evt_block_time >= date_trunc("day", now() - interval '1 week')
  {% endif %}

UNION
 
SELECT
  l.evt_tx_hash as transaction_hash,
  l.evt_block_time as time,
  l.isToken0 as token_0,
  'lend' as transaction_type,
  l.maturity as maturity,
  l.strike as strike, 
  i.pool_pair as pool_pair,
  i.chain as chain,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS DOUBLE) / power(10,i.token0_decimals)
      ELSE CAST(l.tokenAmount AS DOUBLE) / power(10,i.token1_decimals)
    END as DOUBLE
  ) as token_amount,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS DOUBLE) / power(10,i.token0_decimals) * p.price
      ELSE CAST(l.tokenAmount AS DOUBLE) / power(10,i.token1_decimals) * p.price
    END as DOUBLE
  ) as usd_amount
FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryUniswapV3LendGivenPrincipal_evt_LendGivenPrincipal') }} l
JOIN {{ ref('timeswap_polygon_pools') }} i 
  ON CAST(l.maturity as VARCHAR(100)) = i.maturity 
  and cast(l.strike as VARCHAR(100)) = i.strike
JOIN {{ source('prices', 'usd') }} p 
  ON p.symbol=i.token1_symbol 
  and p.blockchain = 'polygon' 
  and l.isToken0 = false
  AND p.minute = date_trunc('minute',l.evt_block_time)
  {% if is_incremental() %}
  AND p.minute >= date_trunc("day", now() - interval '1 week')
WHERE l.evt_block_time >= date_trunc("day", now() - interval '1 week')
  {% endif %}


UNION

SELECT
  l.evt_tx_hash as transaction_hash,
  l.evt_block_time as time,
  l.isToken0 as token_0,
  'lend' as transaction_type,
  l.maturity as maturity,
  l.strike as strike, 
  i.pool_pair as pool_pair,
  i.chain as chain,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS DOUBLE) / power(10,i.token0_decimals)
      ELSE CAST(l.tokenAmount AS DOUBLE) / power(10,i.token1_decimals)
    END as DOUBLE
  ) as token_amount,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS DOUBLE) / power(10,i.token0_decimals) * p.price
      ELSE CAST(l.tokenAmount AS DOUBLE) / power(10,i.token1_decimals) * p.price
    END as DOUBLE
  ) as usd_amount
FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryNoDexLendGivenPrincipal_evt_LendGivenPrincipal') }} l
JOIN {{ ref('timeswap_polygon_pools') }} i 
  ON CAST(l.maturity as VARCHAR(100)) = i.maturity 
  and cast(l.strike as VARCHAR(100)) = i.strike
JOIN {{ source('prices', 'usd') }} p 
  ON p.symbol=i.token0_symbol 
  and p.blockchain = 'polygon' 
  and l.isToken0 = true
  AND p.minute = date_trunc('minute',l.evt_block_time)
  {% if is_incremental() %}
  AND p.minute >= date_trunc("day", now() - interval '1 week')
WHERE l.evt_block_time >= date_trunc("day", now() - interval '1 week')
  {% endif %}

UNION
 
SELECT
  l.evt_tx_hash as transaction_hash,
  l.evt_block_time as time,
  l.isToken0 as token_0,
  'lend' as transaction_type,
  l.maturity as maturity,
  l.strike as strike, 
  i.pool_pair as pool_pair,
  i.chain as chain,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS DOUBLE) / power(10,i.token0_decimals)
      ELSE CAST(l.tokenAmount AS DOUBLE) / power(10,i.token1_decimals)
    END as DOUBLE
  ) as token_amount,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS DOUBLE) / power(10,i.token0_decimals) * p.price
      ELSE CAST(l.tokenAmount AS DOUBLE) / power(10,i.token1_decimals) * p.price
    END as DOUBLE
  ) as usd_amount
FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryNoDexLendGivenPrincipal_evt_LendGivenPrincipal') }} l
JOIN {{ ref('timeswap_polygon_pools') }} i 
  ON CAST(l.maturity as VARCHAR(100)) = i.maturity 
  and cast(l.strike as VARCHAR(100)) = i.strike
JOIN {{ source('prices', 'usd') }} p 
  ON p.symbol=i.token1_symbol 
  and p.blockchain = 'polygon'
  and l.isToken0 = false
  AND p.minute = date_trunc('minute',l.evt_block_time)
  {% if is_incremental() %}
  AND p.minute >= date_trunc("day", now() - interval '1 week')
WHERE l.evt_block_time >= date_trunc("day", now() - interval '1 week')
  {% endif %}



