{{ config(
    alias = 'lend'
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
FROM {{ source('timeswap_arbitrum', 'TimeswapV2PeripheryUniswapV3LendGivenPrincipal_evt_LendGivenPrincipal') }} l
JOIN {{ ref('timeswap_arbitrum_pools') }} i
  ON CAST(l.maturity as UINT256) = i.maturity
  AND CAST(l.strike as UINT256) = i.strike
JOIN {{ source('arbitrum', 'transactions') }} tx
    on l.evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
JOIN {{ source('prices', 'usd') }} p
  ON p.symbol=i.token0_symbol
  AND p.blockchain = 'arbitrum'
  AND l.isToken0 = true
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
FROM {{ source('timeswap_arbitrum', 'TimeswapV2PeripheryUniswapV3LendGivenPrincipal_evt_LendGivenPrincipal') }} l
JOIN {{ ref('timeswap_arbitrum_pools') }} i
  ON CAST(l.maturity as UINT256) = i.maturity
  AND CAST(l.strike as UINT256) = i.strike
JOIN {{ source('arbitrum', 'transactions') }} tx
    on l.evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
JOIN {{ source('prices', 'usd') }} p
  ON p.symbol=i.token1_symbol
  AND p.blockchain = 'arbitrum'
  AND l.isToken0 = false
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
FROM {{ source('timeswap_arbitrum', 'TimeswapV2PeripheryNoDexLendGivenPrincipal_evt_LendGivenPrincipal') }} l
JOIN {{ ref('timeswap_arbitrum_pools') }} i
  ON CAST(l.maturity as UINT256) = i.maturity
  AND CAST(l.strike as UINT256) = i.strike
JOIN {{ source('arbitrum', 'transactions') }} tx
    on l.evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
JOIN {{ source('prices', 'usd') }} p
  ON p.symbol=i.token0_symbol
  AND p.blockchain = 'arbitrum'
  AND l.isToken0 = true
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
FROM {{ source('timeswap_arbitrum', 'TimeswapV2PeripheryNoDexLendGivenPrincipal_evt_LendGivenPrincipal') }} l
JOIN {{ ref('timeswap_arbitrum_pools') }} i
  ON CAST(l.maturity as UINT256) = i.maturity
  AND CAST(l.strike as UINT256) = i.strike
JOIN {{ source('arbitrum', 'transactions') }} tx
    on l.evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
JOIN {{ source('prices', 'usd') }} p
  ON p.symbol=i.token1_symbol
  AND p.blockchain = 'arbitrum'
  AND l.isToken0 = false
  AND p.minute = date_trunc('minute',l.evt_block_time)
{% if is_incremental() %}
  AND p.minute >= date_trunc('day', now() - interval '7' day)
WHERE l.evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}


