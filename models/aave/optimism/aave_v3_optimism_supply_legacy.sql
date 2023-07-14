{{ config(
	tags=['legacy'],
	

    schema = 'aave_v3_optimism'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['version', 'token_address', 'evt_tx_hash', 'evt_block_number', 'evt_index']
    , alias = alias('supply', legacy_model=True)
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
    CAST(reserve AS VARCHAR(100)) AS token,
    user AS depositor, 
    CAST(NULL AS VARCHAR(5)) as withdrawn_to,
    CAST(NULL AS VARCHAR(5)) AS liquidator,
    CAST(amount AS DECIMAL(38,0)) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v3_optimism','Pool_evt_Supply') }}
{% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}
UNION ALL 
SELECT 
    '3' AS version,
    'withdraw' AS transaction_type,
    CAST(reserve AS VARCHAR(100)) AS token,
    user AS depositor,
    CAST(to AS VARCHAR(100)) AS withdrawn_to,
    CAST(NULL AS VARCHAR(5)) AS liquidator,
    - CAST(amount AS DECIMAL(38,0)) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v3_optimism','Pool_evt_Withdraw') }}
{% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}
UNION ALL
SELECT 
    '3' AS version,
    'deposit_liquidation' AS transaction_type,
    CAST(collateralAsset AS VARCHAR(100)) AS token,
    user AS depositor,
    CAST(liquidator AS VARCHAR(100)) AS withdrawn_to,
    CAST(liquidator AS VARCHAR(100)) AS liquidator,
    - CAST(liquidatedCollateralAmount AS DECIMAL(38,0)) AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM {{ source('aave_v3_optimism','Pool_evt_LiquidationCall') }}
{% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}
) deposit
LEFT JOIN {{ ref('tokens_optimism_erc20_legacy') }} erc20
    ON deposit.token = erc20.contract_address
LEFT JOIN {{ source('prices','usd') }} p 
    ON p.minute = date_trunc('minute', deposit.evt_block_time) 
    AND p.contract_address = deposit.token
    AND p.blockchain = 'optimism'
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '1 week')
    {% endif %}
