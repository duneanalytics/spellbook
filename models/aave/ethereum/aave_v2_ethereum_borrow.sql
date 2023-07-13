{{ config(
    schema = 'aave_v2_ethereum'
    , alias='borrow'
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
      amount / CAST(CONCAT('1e',CAST(erc20.decimals AS VARCHAR(100))) AS DOUBLE) AS amount,
      (amount/ CAST(CONCAT('1e',CAST(p.decimals AS VARCHAR(100))) AS DOUBLE)) * price AS usd_amount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number   
FROM (
SELECT 
    '2' AS version,
    'borrow' AS transaction_type,
    CASE 
        WHEN CAST(borrowRateMode AS VARCHAR(100)) = '1' THEN 'stable'
        WHEN CAST(borrowRateMode AS VARCHAR(100)) = '2' THEN 'variable'
    END AS loan_type,
    CAST(reserve AS VARCHAR(100)) AS token,
    CAST(user AS VARCHAR(100)) AS borrower, 
    CAST(NULL AS VARCHAR(5)) AS repayer,
    CAST(NULL AS VARCHAR(5)) AS liquidator,
    CAST(amount AS DECIMAL(38,0)) AS amount,
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
    CAST(reserve AS VARCHAR(100)) AS token,
    CAST(user AS VARCHAR(100)) AS borrower,
    CAST(repayer AS VARCHAR(100)) AS repayer,
    CAST(NULL AS VARCHAR(5)) AS liquidator,
    - CAST(amount AS DECIMAL(38,0)) AS amount,
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
    CAST(debtAsset AS VARCHAR(100)) AS token,
    CAST(user AS VARCHAR(100)) AS borrower,
    CAST(liquidator AS VARCHAR(100)) AS repayer,
    CAST(liquidator AS VARCHAR(100))  AS liquidator,
    - CAST(debtToCover AS DECIMAL(38, 0)) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v2_ethereum','LendingPool_evt_LiquidationCall') }}
) borrow
LEFT JOIN {{ ref('tokens_ethereum_erc20_legacy') }} erc20
    ON borrow.token = erc20.contract_address
LEFT JOIN {{ source('prices','usd') }} p 
    ON p.minute = date_trunc('minute', borrow.evt_block_time) 
    AND CAST(p.contract_address AS VARCHAR(100)) = borrow.token
    AND p.blockchain = 'ethereum'
;