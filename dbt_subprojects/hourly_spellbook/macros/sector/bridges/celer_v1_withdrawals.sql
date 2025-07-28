{% macro celer_v1_withdrawals(blockchain) %}

SELECT i.blockchain AS deposit_chain
, '{{blockchain}}' AS withdrawal_chain
, 'Celer' AS bridge_name
, '1' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS deposit_amount_raw
, sender
, receiver AS recipient
, 'erc20' AS withdrawal_token_standard
, token AS withdrawal_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, CAST(srcTransferId AS varchar) AS bridge_id
FROM {{ source('celer_' + blockchain, 'bridge_evt_relay') }} d
LEFT JOIN {{ source('evms','info') }} i ON d.srcChainId=i.chain_id

{% endmacro %}