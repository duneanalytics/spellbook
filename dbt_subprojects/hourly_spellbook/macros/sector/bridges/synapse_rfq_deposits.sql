{% macro synapse_rfq_deposits(blockchain) %}


SELECT '{{blockchain}}' AS deposit_chain
, d.destChainId AS withdrawal_chain_id
, i.blockchain AS withdrawal_chain
, 'Synapse' AS bridge_name
, 'RFQ' AS bridge_version
, d.evt_block_date AS block_date
, d.evt_block_time AS block_time
, d.evt_block_number AS block_number
, d.originAmount AS deposit_amount_raw
, d.sender AS sender
, CAST(NULL AS varbinary) AS recipient
, 'erc20' AS deposit_token_standard
, 'erc20' AS withdrawal_token_standard
, d.originToken AS deposit_token_address
, d.evt_tx_from AS tx_from
, d.evt_tx_hash AS tx_hash
, d.evt_index
, d.contract_address
, CAST(d.transactionId AS varchar) AS bridge_transfer_id
FROM {{ source('synapse_' + blockchain, 'fastbridge_v2_evt_bridgerequested') }} d
LEFT JOIN {{ ref('evms_info') }} i ON d.destChainId=i.chain_id

{% endmacro %}