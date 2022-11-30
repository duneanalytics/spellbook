{{ config(
    schema = 'aave_v1_ethereum'
    , alias='borrow'
    , post_hook='{{ expose_spells(\'["ethereum"]\',
                                  "project",
                                  "aave_v1",
                                  \'["batwayne", "chuxin"]\') }}'
  )
}}

{% set aave_mock_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %}
{% set weth_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' %}

SELECT
      version,
      transaction_type,
      loan_type,
      erc20.symbol,
      borrow.token as token_address,
      borrower,
      repayer,
      liquidator,
      amount / CAST(CONCAT('1e', CAST(erc20.decimals AS VARCHAR(100))) AS DOUBLE) AS amount,
      (amount/ CAST(CONCAT('1e', CAST(p.decimals AS VARCHAR(1000))) AS DOUBLE)) * price AS usd_amount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number   
FROM (
SELECT 
    '1' AS version,
    'borrow' AS transaction_type,
    CASE 
        WHEN _borrowRateMode = '1' THEN 'stable'
        WHEN _borrowRateMode = '2' THEN 'variable'
    END AS loan_type,
    CASE
        WHEN _reserve = '{{aave_mock_address}}' THEN '{{weth_address}}' --Using WETH instead of Aave "mock" address
        ELSE _reserve
    END AS token,
    _user AS borrower,
    CAST(NULL AS VARCHAR(5)) AS repayer,
    CAST(NULL AS VARCHAR(5)) AS liquidator,
    CAST(_amount AS DECIMAL(38,0)) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_ethereum','LendingPool_evt_Borrow') }} 
UNION ALL 
SELECT 
    '1' AS version,
    'repay' AS transaction_type,
    NULL AS loan_type,
    CASE
        WHEN _reserve = '{{aave_mock_address}}' THEN '{{weth_address}}' --Using WETH instead of Aave "mock" address
        ELSE _reserve
    END AS token,
    _user AS borrower,
    _repayer AS repayer,
    CAST(NULL AS VARCHAR(5)) AS liquidator,
    - CAST(_amountMinusFees AS DECIMAL(38,0)) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_ethereum','LendingPool_evt_Repay') }}
UNION ALL
SELECT 
    '1' AS version,
    'borrow_liquidation' AS transaction_type,
    NULL AS loan_type,
    CASE
        WHEN _reserve = '{{aave_mock_address}}' THEN '{{weth_address}}' --Using WETH instead of Aave "mock" address
        ELSE _reserve
    END AS token,
    _user AS borrower,
    _liquidator AS repayer,
    _liquidator AS liquidator,
    - CAST(_purchaseAmount AS DECIMAL(38,0)) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_ethereum','LendingPool_evt_LiquidationCall') }}
) borrow
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} erc20
    ON borrow.token = erc20.contract_address
LEFT JOIN {{ source('prices','usd') }} p 
    ON p.minute = date_trunc('minute', borrow.evt_block_time) 
    AND p.contract_address = borrow.token
    AND p.blockchain = 'ethereum'    
;