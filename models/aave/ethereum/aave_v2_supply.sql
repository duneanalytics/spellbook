{{ config(
  materialized='view'
  , alias='supply'
  , post_hook='{{ expose_spells(\'["ethereum"]\',
                                  "project",
                                  "aave_v2",
                                  \'["batwayne", "chuxinh"]\') }}'
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
    '1' AS version,
    'deposit' AS transaction_type,
    CASE
        WHEN _reserve = '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' 
        ELSE _reserve
    END AS token,
    _user AS depositor, 
    NULL::string AS withdrawn_to,
    NULL::string AS liquidator,
    _amount AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_ethereum','LendingPool_evt_Deposit') }}
UNION ALL 
SELECT 
    '1' AS version,
    'withdraw' AS transaction_type,
    CASE
        WHEN _reserve = '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' 
        ELSE _reserve
    END AS token,
    _user AS depositor,
    _user AS withdrawn_to,
    NULL::string AS liquidator,
    - _amount AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_ethereum','LendingPool_evt_RedeemUnderlying') }}
UNION ALL 
SELECT 
    '2' AS version,
    'deposit' AS transaction_type,
    reserve AS token,
    user AS depositor, 
    NULL::string as withdrawn_to,
    NULL::string AS liquidator,
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
    NULL::string AS liquidator,
    - amount AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v2_ethereum','LendingPool_evt_Withdraw') }}
UNION ALL
SELECT 
    '1' AS version,
    'deposit_liquidation' AS transaction_type,
    CASE
        WHEN _collateral = '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' 
        ELSE _collateral
    END AS token,
    _user AS depositor,
    _liquidator AS withdrawn_to,
    _liquidator AS liquidator,
    - _liquidatedCollateralAmount AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_ethereum','LendingPool_evt_LiquidationCall') }}
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
    
