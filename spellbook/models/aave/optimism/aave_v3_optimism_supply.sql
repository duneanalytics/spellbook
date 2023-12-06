{{ config(
     schema = 'aave_v3_optimism'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['version', 'token_address', 'evt_tx_hash', 'evt_block_number', 'evt_index']
    , alias = 'supply'
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
      amount / power(10, erc20.decimals) AS amount,
      (amount / power(10, p.decimals)) * price AS usd_amount,
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
    CAST(NULL AS VARBINARY) as withdrawn_to,
    CAST(NULL AS VARBINARY) AS liquidator,
    CAST(amount AS DOUBLE) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v3_optimism','Pool_evt_Supply') }}
{% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
UNION ALL 
SELECT 
    '3' AS version,
    'withdraw' AS transaction_type,
    reserve AS token,
    user AS depositor,
    to AS withdrawn_to,
    CAST(NULL AS VARBINARY) AS liquidator,
    - CAST(amount AS DOUBLE) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v3_optimism','Pool_evt_Withdraw') }}
{% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
UNION ALL
SELECT 
    '3' AS version,
    'deposit_liquidation' AS transaction_type,
    collateralAsset AS token,
    user AS depositor,
    liquidator AS withdrawn_to,
    liquidator AS liquidator,
    - CAST(liquidatedCollateralAmount AS DOUBLE) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v3_optimism','Pool_evt_LiquidationCall') }}
{% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
) deposit
LEFT JOIN {{ ref('tokens_optimism_erc20') }} erc20
    ON deposit.token = erc20.contract_address
LEFT JOIN {{ source('prices','usd') }} p 
    ON p.minute = date_trunc('minute', deposit.evt_block_time) 
    AND p.contract_address = deposit.token
    AND p.blockchain = 'optimism'
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
