{{ config(
    alias = 'lend'
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['transaction_hash', 'pool_pair', 'maturity', 'strike']
    ,post_hook='{{ expose_spells(\'["polygon"]\',
                                "project",
                                "timeswap",
                                \'["raveena15, varunhawk19"]\') }}'
    )
}}




SELECT
    l.call_tx_hash as transaction_hash,
    l.call_block_time as time,
    CAST(JSON_EXTRACT_SCALAR(l.param, '$.isToken') AS BOOLEAN) AS token_0,
    'lend' as transaction_type,
    CAST(JSON_EXTRACT_SCALAR(l.param, '$.maturity') AS UINT256) AS maturity,
    CAST(JSON_EXTRACT_SCALAR(l.param, '$.strike') AS UINT256) AS strike,
    i.pool_pair as pool_pair,
    i.chain as chain,
    tx."from" as user,
    CAST(
        CASE
            WHEN CAST(JSON_EXTRACT_SCALAR(l.param, '$.isToken') AS BOOLEAN) = true
            THEN CAST(JSON_EXTRACT_SCALAR(l.param, '$.tokenAmount') AS UINT256) / power(10,i.token0_decimals)
            ELSE CAST(JSON_EXTRACT_SCALAR(l.param, '$.tokenAmount') AS UINT256) / power(10,i.token1_decimals)
        END as UINT256
    ) as token_amount,
    CAST(
        CASE
            WHEN CAST(JSON_EXTRACT_SCALAR(l.param, '$.isToken') AS BOOLEAN) = true
            THEN CAST(JSON_EXTRACT_SCALAR(l.param, '$.tokenAmount') AS UINT256) / power(10,i.token0_decimals) * p.price
            ELSE CAST(JSON_EXTRACT_SCALAR(l.param, '$.tokenAmount') AS UINT256) / power(10,i.token1_decimals) * p.price
        END as UINT256
    ) as usd_amount
    FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryUniswapV3LendGivenPrincipal_call_lendGivenPrincipal') }} l
    JOIN {{ ref('timeswap_polygon_pools') }} i ON CAST(maturity as UINT256) = i.maturity and cast(strike as UINT256) = i.strike
    JOIN {{ source('polygon', 'transactions') }} tx
      on l.call_tx_hash = tx.hash
      {% if is_incremental() %}
      and tx.block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}
    JOIN {{ source('prices', 'usd') }} p
    ON p.symbol=i.token0_symbol
    and p.blockchain = 'polygon'
    and l.call_success = true
    and CAST(JSON_EXTRACT_SCALAR(l.param, '$.isToken') AS BOOLEAN) = true
    AND p.minute = date_trunc('minute',l.call_block_time)
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' day)
    WHERE l.call_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}


UNION  


SELECT
    l.call_tx_hash as transaction_hash,
    l.call_block_time as time,
    CAST(JSON_EXTRACT_SCALAR(l.param, '$.isToken') AS BOOLEAN) AS token_0,
    'lend' as transaction_type,
    CAST(JSON_EXTRACT_SCALAR(l.param, '$.maturity') AS UINT256) AS maturity,
    CAST(JSON_EXTRACT_SCALAR(l.param, '$.strike') AS UINT256) AS strike,
    i.pool_pair as pool_pair,
    i.chain as chain,
    tx."from" as user,
    CAST(
        CASE
            WHEN CAST(JSON_EXTRACT_SCALAR(l.param, '$.isToken') AS BOOLEAN) = true
            THEN CAST(JSON_EXTRACT_SCALAR(l.param, '$.tokenAmount') AS UINT256) / power(10,i.token0_decimals)
            ELSE CAST(JSON_EXTRACT_SCALAR(l.param, '$.tokenAmount') AS UINT256) / power(10,i.token1_decimals)
        END as UINT256
    ) as token_amount,
    CAST(
        CASE
            WHEN CAST(JSON_EXTRACT_SCALAR(l.param, '$.isToken') AS BOOLEAN) = true
            THEN CAST(JSON_EXTRACT_SCALAR(l.param, '$.tokenAmount') AS UINT256) / power(10,i.token0_decimals) * p.price
            ELSE CAST(JSON_EXTRACT_SCALAR(l.param, '$.tokenAmount') AS UINT256) / power(10,i.token1_decimals) * p.price
        END as UINT256
    ) as usd_amount
    FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryUniswapV3LendGivenPrincipal_call_lendGivenPrincipal') }} l
    JOIN {{ ref('timeswap_polygon_pools') }} i ON CAST(maturity as UINT256) = i.maturity and cast(strike as UINT256) = i.strike
    JOIN {{ source('polygon', 'transactions') }} tx
      on l.call_tx_hash = tx.hash
      {% if is_incremental() %}
      and tx.block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}
    JOIN {{ source('prices', 'usd') }} p
    ON p.symbol=i.token1_symbol
    and p.blockchain = 'polygon'
    and l.call_success = true
    and CAST(JSON_EXTRACT_SCALAR(l.param, '$.isToken') AS BOOLEAN) = false
    AND p.minute = date_trunc('minute',l.call_block_time)
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' day)
    WHERE l.call_block_time >= date_trunc('day', now() - interval '7' day)
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
  tx."from" as user,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS UINT256) / power(10,i.token0_decimals)
      ELSE CAST(l.tokenAmount AS UINT256) / power(10,i.token1_decimals)
    END as UINT256
  ) as token_amount,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS UINT256) / power(10,i.token0_decimals) * p.price
      ELSE CAST(l.tokenAmount AS UINT256) / power(10,i.token1_decimals) * p.price
    END as UINT256
  ) as usd_amount
FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryUniswapV3LendGivenPrincipal_evt_LendGivenPrincipal') }} l
JOIN {{ ref('timeswap_polygon_pools') }} i
  ON CAST(l.maturity as UINT256) = i.maturity
  and CAST(l.strike as UINT256) = i.strike
JOIN {{ source('polygon', 'transactions') }} tx
    on l.evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
JOIN {{ source('prices', 'usd') }} p
  ON p.symbol=i.token0_symbol
  and p.blockchain = 'polygon'
  and l.isToken0 = true
  AND p.minute = date_trunc('minute',l.evt_block_time)
  {% if is_incremental() %}
  AND p.minute >= date_trunc('day', now() - interval '7' day)
WHERE l.evt_block_time >= date_trunc('day', now() - interval '7' day)
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
  tx."from" as user,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS UINT256) / power(10,i.token0_decimals)
      ELSE CAST(l.tokenAmount AS UINT256) / power(10,i.token1_decimals)
    END as UINT256
  ) as token_amount,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS UINT256) / power(10,i.token0_decimals) * p.price
      ELSE CAST(l.tokenAmount AS UINT256) / power(10,i.token1_decimals) * p.price
    END as UINT256
  ) as usd_amount
FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryUniswapV3LendGivenPrincipal_evt_LendGivenPrincipal') }} l
JOIN {{ ref('timeswap_polygon_pools') }} i
  ON CAST(l.maturity as UINT256) = i.maturity
  and CAST(l.strike as UINT256) = i.strike
JOIN {{ source('polygon', 'transactions') }} tx
    on l.evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
JOIN {{ source('prices', 'usd') }} p
  ON p.symbol=i.token1_symbol
  and p.blockchain = 'polygon'
  and l.isToken0 = false
  AND p.minute = date_trunc('minute',l.evt_block_time)
  {% if is_incremental() %}
  AND p.minute >= date_trunc('day', now() - interval '7' day)
WHERE l.evt_block_time >= date_trunc('day', now() - interval '7' day)
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
  tx."from" as user,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS UINT256) / power(10,i.token0_decimals)
      ELSE CAST(l.tokenAmount AS UINT256) / power(10,i.token1_decimals)
    END as UINT256
  ) as token_amount,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS UINT256) / power(10,i.token0_decimals) * p.price
      ELSE CAST(l.tokenAmount AS UINT256) / power(10,i.token1_decimals) * p.price
    END as UINT256
  ) as usd_amount
FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryNoDexLendGivenPrincipal_evt_LendGivenPrincipal') }} l
JOIN {{ ref('timeswap_polygon_pools') }} i
  ON CAST(l.maturity as UINT256) = i.maturity
  and CAST(l.strike as UINT256) = i.strike
JOIN {{ source('polygon', 'transactions') }} tx
    on l.evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
JOIN {{ source('prices', 'usd') }} p
  ON p.symbol=i.token0_symbol
  and p.blockchain = 'polygon'
  and l.isToken0 = true
  AND p.minute = date_trunc('minute',l.evt_block_time)
  {% if is_incremental() %}
  AND p.minute >= date_trunc('day', now() - interval '7' day)
WHERE l.evt_block_time >= date_trunc('day', now() - interval '7' day)
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
  tx."from" as user,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS UINT256) / power(10,i.token0_decimals)
      ELSE CAST(l.tokenAmount AS UINT256) / power(10,i.token1_decimals)
    END as UINT256
  ) as token_amount,
  CAST(
    CASE
      WHEN CAST(l.isToken0 AS BOOLEAN) = true THEN CAST(l.tokenAmount AS UINT256) / power(10,i.token0_decimals) * p.price
      ELSE CAST(l.tokenAmount AS UINT256) / power(10,i.token1_decimals) * p.price
    END as UINT256
  ) as usd_amount
FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryNoDexLendGivenPrincipal_evt_LendGivenPrincipal') }} l
JOIN {{ ref('timeswap_polygon_pools') }} i
  ON CAST(l.maturity as UINT256) = i.maturity
  and CAST(l.strike as UINT256) = i.strike
JOIN {{ source('polygon', 'transactions') }} tx
    on l.evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
JOIN {{ source('prices', 'usd') }} p
  ON p.symbol=i.token1_symbol
  and p.blockchain = 'polygon'
  and l.isToken0 = false
  AND p.minute = date_trunc('minute',l.evt_block_time)
  {% if is_incremental() %}
  AND p.minute >= date_trunc('day', now() - interval '7' day)
WHERE l.evt_block_time >= date_trunc('day', now() - interval '7' day)
  {% endif %}
