{{ config(
     schema = 'aave_v2_ethereum'
    , alias = 'borrow'
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
      amount / power(10, erc20.decimals) AS amount,
      (amount / power(10, p.decimals)) * price AS usd_amount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number   
FROM (
SELECT 
    '2' AS version,
    'borrow' AS transaction_type,
    CASE 
        WHEN borrowRateMode = UINT256 '1' THEN 'stable'
        WHEN borrowRateMode = UINT256 '2' THEN 'variable'
    END AS loan_type,
    reserve AS token,
    user AS borrower, 
    CAST(NULL AS VARBINARY) AS repayer,
    CAST(NULL AS VARBINARY) AS liquidator,
    CAST(amount AS DOUBLE) AS amount,
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
    CAST(NULL AS VARBINARY) AS liquidator,
    - CAST(amount AS DOUBLE) AS amount,
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
    liquidator  AS liquidator,
    - CAST(debtToCover AS DOUBLE) AS amount,
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