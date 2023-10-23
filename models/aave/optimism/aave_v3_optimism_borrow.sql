{{ config(
     schema = 'aave_v3_optimism'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['version', 'token_address', 'evt_tx_hash', 'evt_block_number', 'evt_index']
    , alias = 'borrow'
    , post_hook='{{ expose_spells(\'["optimism"]\',
                                  "project",
                                  "aave_v3",
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
      amount / power(10, erc20.decimals) AS amount,
      (amount / power(10, p.decimals)) * price AS usd_amount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number   
FROM (
SELECT 
    '3' AS version,
    'borrow' AS transaction_type,
    CASE 
        WHEN interestRateMode = 1 THEN 'stable'
        WHEN interestRateMode = 2 THEN 'variable'
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
FROM {{ source('aave_v3_optimism','Pool_evt_Borrow') }} 
{% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
UNION ALL 
SELECT 
    '3' AS version,
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
FROM {{ source('aave_v3_optimism','Pool_evt_Repay') }}
{% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
UNION ALL
SELECT 
    '3' AS version,
    'borrow_liquidation' AS transaction_type,
    NULL AS loan_type,
    debtAsset AS token,
    user AS borrower,
    liquidator AS repayer,
    liquidator AS liquidator,
    - CAST(debtToCover AS DOUBLE) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v3_optimism','Pool_evt_LiquidationCall') }}
{% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
) borrow
LEFT JOIN {{ ref('tokens_optimism_erc20') }} erc20
    ON borrow.token = erc20.contract_address
LEFT JOIN {{ source('prices','usd') }} p 
    ON p.minute = date_trunc('minute', borrow.evt_block_time) 
    AND p.symbol = erc20.symbol 
    AND p.contract_address = borrow.token
    AND p.blockchain = 'optimism'
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
