{% macro symbiosis_v1_deposits(blockchain) %}

SELECT '{{blockchain}}' AS deposit_chain
, d.chainID AS withdrawal_chain_id
, i.blockchain AS withdrawal_chain
, 'Symbiosis' AS bridge_name
, '1' AS bridge_version
, d.evt_block_date AS block_date
, d.evt_block_time AS block_time
, d.evt_block_number AS block_number
, d.amount AS deposit_amount_raw
, "from" AS sender
, to AS recipient
, 'erc20' AS deposit_token_standard
, d.token AS deposit_token_address
, d.evt_tx_from AS tx_from
, d.evt_tx_hash AS tx_hash
, d.evt_index
, d.contract_address
, CAST(d.id AS varchar) AS bridge_transfer_id
FROM {{ source('symbiosis_' + blockchain, 'portal_evt_synthesizerequest') }} d
LEFT JOIN {{ ref('bridges_symbiosis_chain_indexes') }} i ON i.id=d.chainID

{% endmacro %}