{{ config(
    alias = 'borrow',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['Transaction_Hash'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "timeswap",
                                \'["raveena15, varunhawk19"]\') }}'
    )
}}

SELECT
  b.evt_tx_hash as Transaction_Hash,
  b.evt_block_time as Time,
  b.isToken0 as Token_0,
  b.maturity as maturity,
  b.strike as strike,
  'b.from' as User_Address,
  i.pool_pair as Pool_Pair,
  i.chain as Chain,
  CAST(
    CASE
      WHEN CAST(b.isToken0 AS BOOLEAN) = true THEN CAST(b.tokenAmount AS DOUBLE) / power(10,i.token0_decimals)
      ELSE CAST(b.tokenAmount AS DOUBLE) / power(10,i.token1_decimals)
    END as DOUBLE
  ) as Token_Amount,
  CAST(
    CASE
      WHEN CAST(b.isToken0 AS BOOLEAN) = true THEN CAST(b.tokenAmount AS DOUBLE) / power(10,i.token0_decimals) * p.price
      ELSE CAST(b.tokenAmount AS DOUBLE) / power(10,i.token1_decimals) * p.price
    END as DOUBLE
  ) as USD_Amount
  FROM {{ source('timeswap_ethereum', 'TimeswapV2PeripheryUniswapV3BorrowGivenPrincipal_evt_BorrowGivenPrincipal') }} b
  LEFT JOIN {{ ref('timeswap_ethereum_pools') }} i ON CAST(b.contract_address AS VARCHAR(100)) = i.borrow_contract_address
  LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', b.evt_block_time)
  WHERE p.symbol=i.token0_symbol AND p.blockchain = 'ethereum' AND b.isToken0 = true
  {% if is_incremental() %}
    AND b.evt_block_time >= date_trunc("day", now() - interval '1 week')
  {% endif %}

UNION
 
SELECT
  b.evt_tx_hash as Transaction_Hash,
  b.evt_block_time as Time,
  b.isToken0 as Token_0,
  b.maturity as maturity,
  b.strike as strike,
  'b.from' as User_Address,
  i.pool_pair as Pool_Pair,
  i.chain as Chain,
  CAST(
    CASE
      WHEN CAST(b.isToken0 AS BOOLEAN) = true THEN CAST(b.tokenAmount AS DOUBLE) / power(10,i.token0_decimals)
      ELSE CAST(b.tokenAmount AS DOUBLE) / power(10,i.token1_decimals)
    END as DOUBLE
  ) as Token_Amount,
  CAST(
    CASE
      WHEN CAST(b.isToken0 AS BOOLEAN) = true THEN CAST(b.tokenAmount AS DOUBLE) / power(10,i.token0_decimals) * p.price
      ELSE CAST(b.tokenAmount AS DOUBLE) / power(10,i.token1_decimals) * p.price
    END as DOUBLE
  ) as USD_Amount
  FROM {{ source('timeswap_ethereum', 'TimeswapV2PeripheryUniswapV3BorrowGivenPrincipal_evt_BorrowGivenPrincipal') }} b
  LEFT JOIN {{ ref('timeswap_ethereum_pools') }} i ON CAST(b.contract_address AS VARCHAR(100)) = i.borrow_contract_address
  LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', b.evt_block_time)
  WHERE p.symbol=i.token1_symbol AND p.blockchain = 'ethereum' AND b.isToken0 = false
  {% if is_incremental() %}
    AND b.evt_block_time >= date_trunc("day", now() - interval '1 week')
  {% endif %}
