{{ config(
    schema = 'aave_v3_optimism'
    , alias='borrow'
    , post_hook='{{ expose_spells(\'["optimism"]\',
                                  "project",
                                  "aave_v3",
                                  \'["batwayne", "chuxinh"]\') }}'
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
    '3' AS version,
    'borrow' AS transaction_type,
    CASE 
        WHEN interestRateMode = '1' THEN 'stable'
        WHEN interestRateMode = '2' THEN 'variable'
    END AS loan_type,
    reserve AS token,
    user AS borrower, 
    NULL::string AS repayer,
    NULL::string AS liquidator,
    amount, 
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v3_optimism','Pool_evt_Borrow') }} 
UNION ALL 
SELECT 
    '3' AS version,
    'repay' AS transaction_type,
    NULL AS loan_type,
    reserve AS token,
    user AS borrower,
    repayer AS repayer,
    NULL::string AS liquidator,
    - amount AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v3_optimism','Pool_evt_Repay') }}
UNION ALL
SELECT 
    '3' AS version,
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
FROM {{ source('aave_v3_optimism','Pool_evt_LiquidationCall') }}
) borrow
LEFT JOIN {{ ref('tokens_optimism_erc20') }} erc20
    ON borrow.token = erc20.contract_address
LEFT JOIN {{ source('prices','usd') }} p 
    ON p.minute = date_trunc('minute', borrow.evt_block_time) 
    AND p.symbol = erc20.symbol 
    AND p.blockchain = 'ethereum' -- Using ETH tokens for USD prices as price data is not available for OP tokens
