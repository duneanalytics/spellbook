{{ config(
    schema = 'aave_v2_ethereum'
    , alias='supply'
    , post_hook='{{ expose_spells(\'["ethereum"]\',
                                  "project",
                                  "aave_v2",
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
      amount / concat('1e',erc20.decimals) AS amount,
      (amount / concat('1e',p.decimals)) * price AS usd_amount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number 
FROM (
SELECT 
    '2' AS version,
    'deposit' AS transaction_type,
    reserve AS token,
    user AS depositor, 
    CAST(NULL AS VARCHAR(5)) as withdrawn_to,
    CAST(NULL AS VARCHAR(5)) AS liquidator,
    amount, 
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v2_ethereum','LendingPool_evt_Deposit') }}
UNION ALL 
SELECT 
    '2' AS version,
    'withdraw' AS transaction_type,
    reserve AS token,
    user AS depositor,
    to AS withdrawn_to,
    CAST(NULL AS VARCHAR(5)) AS liquidator,
    - amount AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v2_ethereum','LendingPool_evt_Withdraw') }}
UNION ALL
SELECT 
    '2' AS version,
    'deposit_liquidation' AS transaction_type,
    collateralAsset AS token,
    user AS depositor,
    liquidator AS withdrawn_to,
    liquidator AS liquidator,
    - liquidatedCollateralAmount AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v2_ethereum','LendingPool_evt_LiquidationCall') }}
) deposit
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} erc20
    ON deposit.token = erc20.contract_address
LEFT JOIN {{ source('prices','usd') }} p 
    ON p.minute = date_trunc('minute', deposit.evt_block_time) 
    AND p.contract_address = deposit.token 
    AND p.blockchain = 'ethereum'
;