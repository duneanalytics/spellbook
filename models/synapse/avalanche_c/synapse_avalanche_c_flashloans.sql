{{ config(
	tags=['legacy'],
	
    schema = 'synapse_avalanche_c'
    , alias = alias('flashloans', legacy_model=True)
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
  )
}}

{% set blockchain = 'avalanche_c' %}
{% set weth_address = '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7' %}

SELECT '{{blockchain}}' AS blockchain
, 'Synapse' AS project
, 1 AS version
, flash.evt_block_time AS block_time
, flash.evt_block_number AS block_number
, flash.amount/POWER(10, 18) AS amount
, pu.price*(flash.amount/POWER(10, 18)) AS amount_usd
, flash.evt_tx_hash AS tx_hash
, flash.evt_index
, flash.amountFee/POWER(10, 18) AS fee
, '{{weth_address}}' AS currency_contract
, 'AVAX' AS currency_symbol
, flash.receiver AS recipient
, flash.contract_address
FROM {{ source('synapse_avalanche_c','SwapFlashLoan_evt_FlashLoan') }} flash
LEFT JOIN {{ source('prices','usd') }} pu ON pu.blockchain = '{{blockchain}}'
    AND pu.contract_address = '{{weth_address}}'
    AND pu.minute = date_trunc('minute', flash.evt_block_time)
	{% if is_incremental() %}
	AND pu.minute >= date_trunc("day", now() - interval '1 week')
	{% endif %}
{% if is_incremental() %}
WHERE flash.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}