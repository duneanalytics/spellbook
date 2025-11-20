{% macro synapse_rfq_withdrawals(blockchain) %}

SELECT w.originChainId AS deposit_chain_id
, i.blockchain AS deposit_chain
, '{{blockchain}}' AS withdrawal_chain
, 'Synapse' AS bridge_name
, 'RFQ' AS bridge_version
, w.evt_block_date AS block_date
, w.evt_block_time AS block_time
, w.evt_block_number AS block_number
, w.destAmount AS withdrawal_amount_raw
, CAST(NULL AS varbinary) AS sender
, w.to AS recipient
, 'erc20' AS withdrawal_token_standard
, w.destToken AS withdrawal_token_address
, w.evt_tx_from AS tx_from
, w.evt_tx_hash AS tx_hash
, w.evt_index
, w.contract_address
, CAST(w.transactionId AS varchar) AS bridge_transfer_id
FROM {{ source('synapse_' + blockchain, 'fastbridge_v2_evt_bridgerelayed') }} w
LEFT JOIN {{ source('evms','info') }} i ON w.originChainId=i.chain_id

{% endmacro %}