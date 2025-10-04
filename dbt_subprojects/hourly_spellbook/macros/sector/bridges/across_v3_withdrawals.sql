{% macro across_v3_withdrawals(blockchain, events) %}

SELECT m.blockchain AS deposit_chain
, '{{blockchain}}' AS withdrawal_chain
, 'Across' AS bridge_name
, '3' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, outputAmount AS withdrawal_amount_raw
, CASE WHEN varbinary_substring(depositor,1, 12) = 0x000000000000000000000000 THEN varbinary_substring(depositor,13) ELSE depositor END AS sender
, CASE WHEN varbinary_substring(recipient,1, 12) = 0x000000000000000000000000 THEN varbinary_substring(recipient,13) ELSE recipient END AS recipient
, CASE WHEN varbinary_substring(outputToken,1, 12) = 0x000000000000000000000000 THEN varbinary_substring(outputToken,13) ELSE outputToken END AS withdrawal_token_address
, 'erc20' AS deposit_token_standard
, 'erc20' AS withdrawal_token_standard
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, CAST(depositId AS varchar) AS bridge_transfer_id
FROM ({{ events }}) d
LEFT JOIN {{ ref('bridges_cctp_chain_indexes') }} m ON d.originChainId=m.id

{% endmacro %}