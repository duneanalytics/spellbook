{% macro beamer_v2_deposits(blockchain) %}

SELECT '{{blockchain}}' AS deposit_chain
, d.targetChainId AS withdrawal_chain_id
, i.blockchain AS withdrawal_chain
, 'Beamer' AS bridge_name
, '2' AS bridge_version
, d.evt_block_date AS block_date
, d.evt_block_time AS block_time
, evt_block_number AS block_number
, d.amount AS deposit_amount_raw
, d.sourceAddress AS sender
, d.targetAddress AS recipient
, 'erc20' AS deposit_token_standard
, d.sourceTokenAddress AS deposit_token_address
, d.evt_tx_from AS tx_from
, d.evt_tx_hash AS tx_hash
, d.evt_index
, d.contract_address
, CAST(d.requestId AS varchar) AS bridge_transfer_id
FROM {{ source('beamer_bridge_v2_' + blockchain, 'requestmanager_evt_requestcreated') }} d
LEFT JOIN {{ source('evms','info') }} i ON d.targetChainId=i.chain_id

{% endmacro %}