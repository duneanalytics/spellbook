{% macro across_v2_old_deposits(blockchain, events) %}

SELECT '{{blockchain}}' AS deposit_chain
, d.destinationChainId AS withdrawal_chain_id
, m.blockchain AS withdrawal_chain
, 'Across' AS bridge_name
, '2' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS deposit_amount_raw
, depositor AS sender
, recipient
, originToken AS deposit_token_address
, 'erc20' AS deposit_token_standard
, 'erc20' AS withdrawal_token_standard
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, CAST(depositId AS varchar) AS bridge_transfer_id
FROM ({{ events }}) d
LEFT JOIN {{ ref('bridges_across_chain_indexes') }} m ON d.destinationChainId=m.id

{% endmacro %}