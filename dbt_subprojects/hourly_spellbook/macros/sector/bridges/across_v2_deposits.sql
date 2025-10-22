{% macro across_v2_deposits(blockchain, events) %}

SELECT '{{blockchain}}' AS deposit_chain
, d.destinationChainId AS withdrawal_chain_id
, m.blockchain AS withdrawal_chain
, 'Across' AS bridge_name
, '2' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, inputAmount AS deposit_amount_raw
, CASE WHEN varbinary_substring(depositor,1, 12) = 0x000000000000000000000000 THEN varbinary_substring(depositor,13) ELSE depositor END AS sender
, CASE WHEN varbinary_substring(recipient,1, 12) = 0x000000000000000000000000 THEN varbinary_substring(recipient,13) ELSE recipient END AS recipient
, CASE WHEN varbinary_substring(inputToken,1, 12) = 0x000000000000000000000000 THEN varbinary_substring(inputToken,13) ELSE inputToken END AS deposit_token_address
, 'erc20' AS deposit_token_standard
, 'erc20' AS withdrawal_token_standard
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
{% if blockchain == 'unichain' %}
, CAST(depositId AS varchar) AS bridge_transfer_id
{% else %}
, CAST(depositId_uint256 AS varchar) AS bridge_transfer_id
{% endif %}
FROM ({{ events }}) d
LEFT JOIN {{ ref('bridges_across_chain_indexes') }} m ON d.destinationChainId=m.id

{% endmacro %}