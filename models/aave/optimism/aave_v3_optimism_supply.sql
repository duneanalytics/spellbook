{{ config(
    schema = 'aave_v3_optimism'
    , alias='supply'
    , post_hook='{{ expose_spells(\'["optimism"]\',
                                  "project",
                                  "aave_v3",
                                  \'["batwayne", "chuxin"]\') }}'
  )
}}

SELECT 
      version,
      transaction_type,
      erc20.symbol,
      deposit.token as token_address, 
      depositor,
      withdrawn_to,
      liquidator,
      amount / CAST(CONCAT('1e',CAST(erc20.decimals AS VARCHAR(100))) AS DOUBLE) AS amount,
      (amount / CAST(CONCAT('1e',CAST(p.decimals AS VARCHAR(100))) AS DOUBLE)) * price AS usd_amount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number 
FROM (
SELECT 
    '3' AS version,
    'deposit' AS transaction_type,
    reserve AS token,
    user AS depositor, 
    CAST(NULL AS VARCHAR(5)) as withdrawn_to,
    CAST(NULL AS VARCHAR(5)) AS liquidator,
    CAST(amount AS DECIMAL(38,0)) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v3_optimism','Pool_evt_Supply') }}
UNION ALL 
SELECT 
    '3' AS version,
    'withdraw' AS transaction_type,
    reserve AS token,
    user AS depositor,
    to AS withdrawn_to,
    CAST(NULL AS VARCHAR(5)) AS liquidator,
    - CAST(amount AS DECIMAL(38,0)) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v3_optimism','Pool_evt_Withdraw') }}
UNION ALL
SELECT 
    '3' AS version,
    'deposit_liquidation' AS transaction_type,
    collateralAsset AS token,
    user AS depositor,
    liquidator AS withdrawn_to,
    liquidator AS liquidator,
    - CAST(liquidatedCollateralAmount AS DECIMAL(38,0)) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v3_optimism','Pool_evt_LiquidationCall') }}
) deposit
LEFT JOIN {{ ref('tokens_optimism_erc20') }} erc20
    ON deposit.token = erc20.contract_address
LEFT JOIN {{ source('prices','usd') }} p 
    ON p.minute = date_trunc('minute', deposit.evt_block_time) 
    AND p.symbol = erc20.symbol 
    AND p.blockchain = 'ethereum' -- Using ETH tokens for USD prices as price data is not available for OP tokens
