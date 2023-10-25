{{ config(
    alias = 'borrow'
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
    b.call_tx_hash as transaction_hash,
    b.call_block_time as time,
    CAST(JSON_EXTRACT_SCALAR(b.param, '$.isToken') AS BOOLEAN) AS token_0,
    'borrow' as transaction_type,
    CAST(JSON_EXTRACT_SCALAR(b.param, '$.maturity') AS UINT256) AS maturity,
    CAST(JSON_EXTRACT_SCALAR(b.param, '$.strike') AS UINT256) AS strike,
    i.pool_pair as pool_pair,
    i.chain as chain,
    tx."from" as user,
    CAST(
        CASE
            WHEN CAST(JSON_EXTRACT_SCALAR(b.param, '$.isToken') AS BOOLEAN) = true
            THEN CAST(JSON_EXTRACT_SCALAR(b.param, '$.tokenAmount') AS UINT256) / power(10,i.token0_decimals)
            ELSE CAST(JSON_EXTRACT_SCALAR(b.param, '$.tokenAmount') AS UINT256) / power(10,i.token1_decimals)
        END as UINT256
    ) as token_amount,
    CAST(
        CASE
            WHEN CAST(JSON_EXTRACT_SCALAR(b.param, '$.isToken') AS BOOLEAN) = true
            THEN CAST(JSON_EXTRACT_SCALAR(b.param, '$.tokenAmount') AS UINT256) / power(10,i.token0_decimals) * p.price
            ELSE CAST(JSON_EXTRACT_SCALAR(b.param, '$.tokenAmount') AS UINT256) / power(10,i.token1_decimals) * p.price
        END as UINT256
    ) as usd_amount
    FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryUniswapV3BorrowGivenPrincipal_call_borrowGivenPrincipal') }} b
    JOIN {{ ref('timeswap_polygon_pools') }} i ON CAST(maturity as UINT256) = i.maturity and cast(strike as UINT256) = i.strike
    JOIN {{ source('polygon', 'transactions') }} tx
    on b.call_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    JOIN {{ source('prices', 'usd') }} p
    ON p.symbol=i.token0_symbol
    and p.blockchain = 'polygon'
    and b.call_success = true
    and CAST(JSON_EXTRACT_SCALAR(b.param, '$.isToken') AS BOOLEAN) = true
    AND p.minute = date_trunc('minute',b.call_block_time)
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' day)
    WHERE b.call_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}


UNION  


SELECT
    b.call_tx_hash as transaction_hash,
    b.call_block_time as time,
    CAST(JSON_EXTRACT_SCALAR(b.param, '$.isToken') AS BOOLEAN) AS token_0,
    'borrow' as transaction_type,
    CAST(JSON_EXTRACT_SCALAR(b.param, '$.maturity') AS UINT256) AS maturity,
    CAST(JSON_EXTRACT_SCALAR(b.param, '$.strike') AS UINT256) AS strike,
    i.pool_pair as pool_pair,
    i.chain as chain,
    tx."from" as user,
    CAST(
        CASE
            WHEN CAST(JSON_EXTRACT_SCALAR(b.param, '$.isToken') AS BOOLEAN) = true
            THEN CAST(JSON_EXTRACT_SCALAR(b.param, '$.tokenAmount') AS UINT256) / power(10,i.token0_decimals)
            ELSE CAST(JSON_EXTRACT_SCALAR(b.param, '$.tokenAmount') AS UINT256) / power(10,i.token1_decimals)
        END as UINT256
    ) as token_amount,
    CAST(
        CASE
            WHEN CAST(JSON_EXTRACT_SCALAR(b.param, '$.isToken') AS BOOLEAN) = true
            THEN CAST(JSON_EXTRACT_SCALAR(b.param, '$.tokenAmount') AS UINT256) / power(10,i.token0_decimals) * p.price
            ELSE CAST(JSON_EXTRACT_SCALAR(b.param, '$.tokenAmount') AS UINT256) / power(10,i.token1_decimals) * p.price
        END as UINT256
    ) as usd_amount
    FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryUniswapV3BorrowGivenPrincipal_call_borrowGivenPrincipal') }} b
    JOIN {{ ref('timeswap_polygon_pools') }} i ON CAST(maturity as UINT256) = i.maturity and cast(strike as UINT256) = i.strike
    JOIN {{ source('polygon', 'transactions') }} tx
    on b.call_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    JOIN {{ source('prices', 'usd') }} p
    ON p.symbol=i.token1_symbol
    and p.blockchain = 'polygon'
    and b.call_success = true
    and CAST(JSON_EXTRACT_SCALAR(b.param, '$.isToken') AS BOOLEAN) = false
    AND p.minute = date_trunc('minute',b.call_block_time)
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' day)
    WHERE b.call_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}


UNION


SELECT
  b.evt_tx_hash as transaction_hash,
  b.evt_block_time as time,
  b.isToken0 as token_0,
  'borrow' as transaction_type,
  b.maturity as maturity,
  b.strike as strike,
  i.pool_pair as pool_pair,
  i.chain as chain,
  tx."from" as user,
  CAST(
    CASE
      WHEN CAST(b.isToken0 AS BOOLEAN) = true THEN CAST(b.tokenAmount AS UINT256) / power(10,i.token0_decimals)
      ELSE CAST(b.tokenAmount AS UINT256) / power(10,i.token1_decimals)
    END as UINT256
  ) as token_amount,
  CAST(
    CASE
      WHEN CAST(b.isToken0 AS BOOLEAN) = true THEN CAST(b.tokenAmount AS UINT256) / power(10,i.token0_decimals) * p.price
      ELSE CAST(b.tokenAmount AS UINT256) / power(10,i.token1_decimals) * p.price
    END as UINT256
  ) as usd_amount
FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryUniswapV3BorrowGivenPrincipal_evt_BorrowGivenPrincipal') }} b
JOIN {{ ref('timeswap_polygon_pools') }} i
  ON CAST(b.maturity as UINT256) = i.maturity
  and cast(b.strike as UINT256) = i.strike
JOIN {{ source('polygon', 'transactions') }} tx
    on b.evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
JOIN {{ source('prices', 'usd') }} p
  ON p.symbol=i.token0_symbol
  and p.blockchain = 'polygon'
  and b.isToken0 = true
  AND p.minute = date_trunc('minute',b.evt_block_time)
  {% if is_incremental() %}
  AND p.minute >= date_trunc('day', now() - interval '7' day)
WHERE b.evt_block_time >= date_trunc('day', now() - interval '7' day)
  {% endif %}


UNION
 
SELECT
  b.evt_tx_hash as transaction_hash,
  b.evt_block_time as time,
  b.isToken0 as token_0,
  'borrow' as transaction_type,
  b.maturity as maturity,
  b.strike as strike,
  i.pool_pair as pool_pair,
  i.chain as chain,
  tx."from" as user,
  CAST(
    CASE
      WHEN CAST(b.isToken0 AS BOOLEAN) = true THEN CAST(b.tokenAmount AS UINT256) / power(10,i.token0_decimals)
      ELSE CAST(b.tokenAmount AS UINT256) / power(10,i.token1_decimals)
    END as UINT256
  ) as token_amount,
  CAST(
    CASE
      WHEN CAST(b.isToken0 AS BOOLEAN) = true THEN CAST(b.tokenAmount AS UINT256) / power(10,i.token0_decimals) * p.price
      ELSE CAST(b.tokenAmount AS UINT256) / power(10,i.token1_decimals) * p.price
    END as UINT256
  ) as usd_Amount
FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryUniswapV3BorrowGivenPrincipal_evt_BorrowGivenPrincipal') }} b
JOIN {{ ref('timeswap_polygon_pools') }} i
  ON CAST(b.maturity as UINT256) = i.maturity
  and cast(b.strike as UINT256) = i.strike
JOIN {{ source('polygon', 'transactions') }} tx
    on b.evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
JOIN {{ source('prices', 'usd') }} p
  ON p.symbol=i.token1_symbol
  and p.blockchain = 'polygon'
  and b.isToken0 = false
  AND p.minute = date_trunc('minute',b.evt_block_time)
  {% if is_incremental() %}
  AND p.minute >= date_trunc('day', now() - interval '7' day)
WHERE b.evt_block_time >= date_trunc('day', now() - interval '7' day)
  {% endif %}




UNION


SELECT
  b.evt_tx_hash as transaction_hash,
  b.evt_block_time as time,
  b.isToken0 as token_0,
  'borrow' as transaction_type,
  b.maturity as maturity,
  b.strike as strike,
  i.pool_pair as pool_pair,
  i.chain as chain,
  tx."from" as user,
  CAST(
    CASE
      WHEN CAST(b.isToken0 AS BOOLEAN) = true THEN CAST(b.tokenAmount AS UINT256) / power(10,i.token0_decimals)
      ELSE CAST(b.tokenAmount AS UINT256) / power(10,i.token1_decimals)
    END as UINT256
  ) as token_amount,
  CAST(
    CASE
      WHEN CAST(b.isToken0 AS BOOLEAN) = true THEN CAST(b.tokenAmount AS UINT256) / power(10,i.token0_decimals) * p.price
      ELSE CAST(b.tokenAmount AS UINT256) / power(10,i.token1_decimals) * p.price
    END as UINT256
  ) as usd_amount
FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryNoDexBorrowGivenPrincipal_evt_BorrowGivenPrincipal') }} b
JOIN {{ ref('timeswap_polygon_pools') }} i
  ON CAST(b.maturity as UINT256) = i.maturity
  and cast(b.strike as UINT256) = i.strike
JOIN {{ source('polygon', 'transactions') }} tx
    on b.evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
JOIN {{ source('prices', 'usd') }} p
  ON p.symbol=i.token0_symbol
  and p.blockchain = 'polygon'
  and b.isToken0 = true
  AND p.minute = date_trunc('minute',b.evt_block_time)
  {% if is_incremental() %}
  AND p.minute >= date_trunc('day', now() - interval '7' day)
WHERE b.evt_block_time >= date_trunc('day', now() - interval '7' day)
  {% endif %}


UNION
 
SELECT
  b.evt_tx_hash as transaction_hash,
  b.evt_block_time as time,
  b.isToken0 as token_0,
  'borrow' as transaction_type,
  b.maturity as maturity,
  b.strike as strike,
  i.pool_pair as pool_pair,
  i.chain as chain,
  tx."from" as user,
  CAST(
    CASE
      WHEN CAST(b.isToken0 AS BOOLEAN) = true THEN CAST(b.tokenAmount AS UINT256) / power(10,i.token0_decimals)
      ELSE CAST(b.tokenAmount AS UINT256) / power(10,i.token1_decimals)
    END as UINT256
  ) as token_amount,
  CAST(
    CASE
      WHEN CAST(b.isToken0 AS BOOLEAN) = true THEN CAST(b.tokenAmount AS UINT256) / power(10,i.token0_decimals) * p.price
      ELSE CAST(b.tokenAmount AS UINT256) / power(10,i.token1_decimals) * p.price
    END as UINT256
  ) as usd_amount
FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryNoDexBorrowGivenPrincipal_evt_BorrowGivenPrincipal') }} b
JOIN {{ ref('timeswap_polygon_pools') }} i
  ON CAST(b.maturity as UINT256) = i.maturity
  and cast(b.strike as UINT256) = i.strike
JOIN {{ source('polygon', 'transactions') }} tx
    on b.evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
JOIN {{ source('prices', 'usd') }} p
  ON p.symbol=i.token1_symbol
  and p.blockchain = 'polygon'
  and b.isToken0 = false
  AND p.minute = date_trunc('minute',b.evt_block_time)
  {% if is_incremental() %}
  AND p.minute >= date_trunc('day', now() - interval '7' day)
WHERE b.evt_block_time >= date_trunc('day', now() - interval '7' day)
  {% endif %}
