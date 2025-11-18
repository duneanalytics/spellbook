{% macro allbridge_classic_deposits(blockchain) %}

SELECT '{{blockchain}}' AS deposit_chain
, i.chain_id AS withdrawal_chain_id
, ci.blockchain AS withdrawal_chain
, 'Allbridge' AS bridge_name
, 'Classic' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS deposit_amount_raw
, sender
, CASE WHEN substr(recipient, 21) = 0x000000000000000000000000 THEN substr(recipient, 1, 20) ELSE recipient END AS recipient
, nativeTokenAddress AS deposit_token_address
, 'erc20' AS deposit_token_standard
, 'erc20' AS withdrawal_token_standard
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, d.contract_address
, CAST(lockId AS varchar) AS bridge_transfer_id
FROM ({{ source('allbridge_' + blockchain, 'bridge_evt_sent') }}) d
LEFT JOIN {{ source('allbridge_' + blockchain, 'bridge_call_addtoken') }} at USING (tokenSource)
LEFT JOIN {{ ref('bridges_allbridge_classic_chain_indexes') }} ci ON trim(from_utf8(d.destination))=ci.allbridge_slug
LEFT JOIN {{ source('evms','info') }} i ON ci.blockchain=i.blockchain

{% endmacro %}