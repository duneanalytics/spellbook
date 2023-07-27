{{ config(
	tags=['legacy'],
	
      alias = alias('flashloans', legacy_model=True)
      , materialized = 'incremental'
      , file_format = 'delta'
      , incremental_strategy = 'merge'
      , unique_key = ['tx_hash', 'evt_index']
      , post_hook='{{ expose_spells(\'["ethereum"]\',
                                  "project",
                                  "fiat_dao",
                                  \'["hildobby"]\') }}'
  )
}}

SELECT 'ethereum' AS blockchain
, 'Fiat DAO' AS project
, 1 AS version
, flash.evt_block_time AS block_time
, flash.evt_block_number AS block_number
, flash.amount/POWER(10, 18) AS amount
, pu.price*(flash.amount/POWER(10, 18)) AS amount_usd
, flash.evt_tx_hash AS tx_hash
, flash.evt_index
, 0 AS fee
, flash.token AS currency_contract
, tok.symbol AS currency_symbol
, flash.receiver AS recipient
, flash.contract_address
FROM {{ source('fiat_dao_ethereum','Flash_evt_FlashLoan') }} flash
LEFT JOIN {{ ref('tokens_ethereum_erc20_legacy') }} tok ON flash.token=tok.contract_address
LEFT JOIN {{ source('prices','usd') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address = flash.token
    AND pu.minute = date_trunc('minute', flash.evt_block_time)
    {% if is_incremental() %}
    AND pu.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
{% if is_incremental() %}
AND flash.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}