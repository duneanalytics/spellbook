{% macro celer_v1_deposits(blockchain) %}

SELECT '{{blockchain}}' AS deposit_chain
, i.blockchain AS withdrawal_chain
, 'Celer' AS bridge_name
, '1' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS deposit_amount_raw
, sender
, receiver AS recipient
, 'erc20' AS deposit_token_standard
, token AS deposit_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, CAST(transferId AS varchar) AS bridge_id
FROM {{ source('celer_' + blockchain, 'bridge_evt_send') }} d
LEFT JOIN {{ source('evms','info') }} i ON d.dstChainId=i.chain_id

{% endmacro %}