{% macro across_v1_withdrawals(blockchain, events) %}

SELECT d.originChainId AS deposit_chain_id
, m.blockchain AS deposit_chain
, '{{blockchain}}' AS withdrawal_chain
, 'Across' AS bridge_name
, '1' AS bridge_version
, d.evt_block_date AS block_date
, d.evt_block_time AS block_time
, d.evt_block_number AS block_number
, d.amount AS withdrawal_amount_raw
, depositor AS sender
, recipient AS recipient
, destinationToken AS withdrawal_token_address
, 'erc20' AS deposit_token_standard
, 'erc20' AS withdrawal_token_standard
, d.evt_tx_from AS tx_from
, d.evt_tx_hash AS tx_hash
, d.evt_index
, d.contract_address
, CAST(d.depositId AS varchar) AS bridge_transfer_id
FROM ({{ events }}) d
LEFT JOIN {{ ref('bridges_across_chain_indexes') }} m ON d.originChainId=m.id

{% endmacro %}