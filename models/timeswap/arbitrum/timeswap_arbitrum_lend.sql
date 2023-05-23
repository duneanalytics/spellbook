{{ config(
    alias = 'lend',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['Transaction_Hash'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "timeswap",
                                \'["raveena15, varunhawk19"]\') }}'
    )
}}

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
  FROM {{ source('timeswap_arbitrum', 'TimeswapV2PeripheryUniswapV3LendGivenPrincipal_evt_LendGivenPrincipal') }} l
  JOIN {{ ref('timeswap_arbitrum_pools') }} i ON CAST(l.maturity as VARCHAR(100)) = i.maturity and cast(l.strike as VARCHAR(100)) = i.strike
  JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', l.evt_block_time)
  WHERE p.symbol=i.token0_symbol AND p.blockchain = 'arbitrum' AND l.isToken0 = true
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
  FROM {{ source('timeswap_arbitrum', 'TimeswapV2PeripheryUniswapV3LendGivenPrincipal_evt_LendGivenPrincipal') }} l
  JOIN {{ ref('timeswap_arbitrum_pools') }} i ON CAST(l.maturity as VARCHAR(100)) = i.maturity and cast(l.strike as VARCHAR(100)) = i.strike
  JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', l.evt_block_time)
  WHERE p.symbol=i.token1_symbol AND p.blockchain = 'arbitrum' AND l.isToken0 = false
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
  FROM {{ source('timeswap_arbitrum', 'TimeswapV2PeripheryNoDexLendGivenPrincipal_evt_LendGivenPrincipal') }} l
  JOIN {{ ref('timeswap_arbitrum_pools') }} i ON CAST(l.maturity as VARCHAR(100)) = i.maturity and cast(l.strike as VARCHAR(100)) = i.strike
  JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', l.evt_block_time)
  WHERE p.symbol=i.token0_symbol AND p.blockchain = 'arbitrum' AND l.isToken0 = true
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
  FROM {{ source('timeswap_arbitrum', 'TimeswapV2PeripheryNoDexLendGivenPrincipal_evt_LendGivenPrincipal') }} l
  JOIN {{ ref('timeswap_arbitrum_pools') }} i ON CAST(l.maturity as VARCHAR(100)) = i.maturity and cast(l.strike as VARCHAR(100)) = i.strike
  JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', l.evt_block_time)
  WHERE p.symbol=i.token0_symbol AND p.blockchain = 'arbitrum' AND l.isToken0 = true
  {% if is_incremental() %}
    AND l.evt_block_time >= date_trunc("day", now() - interval '1 week')
  {% endif %}



