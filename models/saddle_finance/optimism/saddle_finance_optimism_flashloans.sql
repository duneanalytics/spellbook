{{ config(
      alias='flashloans'
      , materialized = 'incremental'
      , file_format = 'delta'
      , incremental_strategy = 'merge'
      , unique_key = ['tx_hash', 'evt_index']
      , post_hook='{{ expose_spells(\'["optimism"]\',
                                  "project",
                                  "saddle_finance",
                                  \'["hildobby"]\') }}'
  )
}}

WITH flashloans AS (
    SELECT flash.evt_block_time AS block_time
    , flash.evt_block_number AS block_number
    , flash.amount AS amount_raw
    , flash.evt_tx_hash AS tx_hash
    , flash.evt_index
    , flash.amountFee AS fee_raw
    , get_json_object(pool.poolData, "$.tokens[" || CAST(flash.tokenIndex AS string) || "]") AS currency_contract
    , flash.receiver AS recipient
    , flash.contract_address
    FROM {{ source('saddle_finance_optimism','SwapFlashLoan_evt_FlashLoan') }} flash
    INNER JOIN {{ source('saddle_finance_optimism','PoolRegistry_evt_AddPool') }} pool ON pool.poolAddress=flash.contract_address
    {% if is_incremental() %}
    WHERE flash.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    )

SELECT 'optimism' AS blockchain
, 'Saddle' AS project
, '1' AS version
, flash.block_time
, flash.block_number
, flash.amount_raw/POWER(10, tok.decimals) AS amount
, pu.price*(flash.amount_raw/POWER(10, tok.decimals)) AS amount_usd
, flash.tx_hash
, flash.evt_index
, flash.fee_raw/POWER(10, tok.decimals) AS fee
, flash.currency_contract
, tok.symbol AS currency_symbol
, flash.recipient
, flash.contract_address
FROM flashloans flash
LEFT JOIN {{ ref('tokens_optimism_erc20') }} tok ON tok.contract_address=flash.currency_contract
LEFT JOIN {{ source('prices','usd') }} pu ON pu.blockchain = 'optimism'  
  AND pu.contract_address=flash.currency_contract
  AND pu.minute = date_trunc('minute', flash.block_time)
  {% if is_incremental() %}
  AND pu.minute >= date_trunc("day", now() - interval '1 week')
  {% endif %}