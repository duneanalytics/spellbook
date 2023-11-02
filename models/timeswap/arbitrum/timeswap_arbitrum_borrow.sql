{{ config(
    alias = 'borrow'
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['transaction_hash', 'pool_pair', 'maturity', 'strike']
    ,post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "timeswap",
                                \'["raveena15, varunhawk19"]\') }}'
    )
}}




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
FROM {{ source('timeswap_arbitrum', 'TimeswapV2PeripheryUniswapV3BorrowGivenPrincipal_evt_BorrowGivenPrincipal') }} b
JOIN {{ ref('timeswap_arbitrum_pools') }} i
    ON CAST(b.maturity as UINT256) = i.maturity
    AND cast(b.strike as UINT256) = i.strike
JOIN {{ source('arbitrum', 'transactions') }} tx
    on b.evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
JOIN {{ source('prices', 'usd') }} p
  ON p.symbol=i.token0_symbol
  AND p.blockchain = 'arbitrum'
  AND b.isToken0 = true
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
FROM {{ source('timeswap_arbitrum', 'TimeswapV2PeripheryUniswapV3BorrowGivenPrincipal_evt_BorrowGivenPrincipal') }} b
JOIN {{ ref('timeswap_arbitrum_pools') }} i
  ON CAST(b.maturity as UINT256) = i.maturity
  AND cast(b.strike as UINT256) = i.strike
JOIN {{ source('arbitrum', 'transactions') }} tx
    on b.evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
JOIN {{ source('prices', 'usd') }} p
  ON p.symbol=i.token1_symbol
  AND p.blockchain = 'arbitrum'
  AND b.isToken0 = false
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
FROM {{ source('timeswap_arbitrum', 'TimeswapV2PeripheryNoDexBorrowGivenPrincipal_evt_BorrowGivenPrincipal') }} b
JOIN {{ ref('timeswap_arbitrum_pools') }} i
  ON CAST(b.maturity as UINT256) = i.maturity
  AND cast(b.strike as UINT256) = i.strike
JOIN {{ source('arbitrum', 'transactions') }} tx
    on b.evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
JOIN {{ source('prices', 'usd') }} p
  ON p.symbol=i.token0_symbol
  AND p.blockchain = 'arbitrum'
  AND b.isToken0 = true
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
FROM {{ source('timeswap_arbitrum', 'TimeswapV2PeripheryNoDexBorrowGivenPrincipal_evt_BorrowGivenPrincipal') }} b
JOIN {{ ref('timeswap_arbitrum_pools') }} i
  ON CAST(b.maturity as UINT256) = i.maturity
  AND cast(b.strike as UINT256) = i.strike
JOIN {{ source('arbitrum', 'transactions') }} tx
    on b.evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
JOIN {{ source('prices', 'usd') }} p
  ON p.symbol=i.token1_symbol
  AND p.blockchain = 'arbitrum'
  AND b.isToken0 = false
  AND p.minute = date_trunc('minute',b.evt_block_time)
  {% if is_incremental() %}
  AND p.minute >= date_trunc('day', now() - interval '7' day)
WHERE b.evt_block_time >= date_trunc('day', now() - interval '7' day)
  {% endif %}


