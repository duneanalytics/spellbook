{{ config(
     partition_by = ['block_month']
      , schema = 'balancer_v2_polygon'
      , alias = 'flashloans'
      , materialized = 'incremental'
      , file_format = 'delta'
      , incremental_strategy = 'merge'
      , unique_key = ['tx_hash', 'evt_index']
      , post_hook='{{ expose_spells(\'["polygon"]\',
                                  "project",
                                  "balancer_v2",
                                  \'["hildobby"]\') }}'
  )
}}

WITH flashloans AS (
    SELECT f.evt_block_time AS block_time
    , f.evt_block_number AS block_number
    , f.amount AS amount_raw
    , f.evt_tx_hash AS tx_hash
    , f.evt_index
    , f.feeAmount AS fee
    , f.token AS currency_contract
    , erc20.symbol AS currency_symbol
    , erc20.decimals AS currency_decimals
    , f.contract_address
    FROM {{ source('balancer_v2_polygon','Vault_evt_FlashLoan') }} f
    LEFT JOIN {{ source('tokens_polygon', 'erc20') }} erc20 ON f.token = erc20.contract_address
        {% if is_incremental() %}
        WHERE f.evt_block_time >= date_trunc('day', now() - interval '7' Day)
        {% endif %}
    )

SELECT 'polygon' AS blockchain
, 'balancer' AS project
, '2' AS version
, CAST(date_trunc('Month', flash.block_time) as date) as block_month
, flash.block_time
, flash.block_number
, flash.amount_raw/POWER(10, flash.currency_decimals) AS amount
, pu.price*flash.amount_raw/POWER(10, flash.currency_decimals) AS amount_usd
, flash.tx_hash
, flash.evt_index
, flash.fee/POWER(10, flash.currency_decimals) AS fee
, flash.currency_contract
, flash.currency_symbol
, flash.contract_address
FROM flashloans flash
LEFT JOIN {{ source('prices','usd') }} pu ON pu.blockchain = 'polygon'  
    AND pu.contract_address = flash.currency_contract
    AND pu.minute = date_trunc('minute', flash.block_time)