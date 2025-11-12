{% macro across_v1_deposits(blockchain) %}

SELECT '{{blockchain}}' AS deposit_chain
, i.chain_id AS withdrawal_chain_id
, ci.blockchain AS withdrawal_chain
, 'Across' AS bridge_name
, '1' AS bridge_version
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
FROM ({{ source('allbridge_' + blockchain, '_bridge_evt_sent') }}) d
LEFT JOIN {{ ref('bridges_allbridge_classic_chain_indexes') }} ci ON trim(from_utf8(d.destination))=ci.allbridge_slug
LEFT JOIN {{ source('evms','info') }} i ON ci.blockchain=i.blockchain

{% endmacro %}