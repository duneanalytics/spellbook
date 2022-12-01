{{ config(
    schema = 'aave_v2_ethereum'
    , alias='borrow'
    , post_hook='{{ expose_spells(\'["ethereum"]\',
                                  "project",
                                  "aave_v2",
                                  \'["batwayne", "chuxin"]\') }}'
  )
}}

SELECT
      version,
      transaction_type,
      loan_type,
      erc20.symbol,
      borrow.token as token_address,
      borrower,
      repayer,
      liquidator,
      amount / concat('1e',erc20.decimals) AS amount,
      (amount/ concat('1e',p.decimals)) * price AS usd_amount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number   
FROM (
SELECT 
    '2' AS version,
    'borrow' AS transaction_type,
    CASE 
        WHEN borrowRateMode = '1' THEN 'stable'
        WHEN borrowRateMode = '2' THEN 'variable'
    END AS loan_type,
    reserve AS token,
    user AS borrower, 
    CAST(NULL AS VARCHAR(5)) AS repayer,
    CAST(NULL AS VARCHAR(5)) AS liquidator,
    amount, 
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v2_ethereum','LendingPool_evt_Borrow') }} 
UNION ALL 
SELECT 
    '2' AS version,
    'repay' AS transaction_type,
    NULL AS loan_type,
    reserve AS token,
    user AS borrower,
    repayer AS repayer,
    CAST(NULL AS VARCHAR(5)) AS liquidator,
    - amount AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v2_ethereum','LendingPool_evt_Repay') }}
UNION ALL
SELECT 
    '2' AS version,
    'borrow_liquidation' AS transaction_type,
    NULL AS loan_type,
    debtAsset AS token,
    user AS borrower,
    liquidator AS repayer,
    liquidator AS liquidator,
    - debtToCover AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v2_ethereum','LendingPool_evt_LiquidationCall') }}
) borrow
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} erc20
    ON borrow.token = erc20.contract_address
LEFT JOIN {{ source('prices','usd') }} p 
    ON p.minute = date_trunc('minute', borrow.evt_block_time) 
    AND p.contract_address = borrow.token
    AND p.blockchain = 'ethereum'
;