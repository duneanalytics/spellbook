{% macro butter_v2_deposits(blockchain) %}

SELECT '{{blockchain}}' AS deposit_chain
, d.fromChain
, d.toChain AS withdrawal_chain_id
, m.blockchain AS withdrawal_chain
, 'Butter' AS bridge_name
, '2.1' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, originAmount AS deposit_amount_raw
, bridgeAmount AS withdrawal_amount_raw
, "from" AS sender
, to AS recipient
, d.originToken AS deposit_token_address
, d.bridgeToken AS withdrawal_token_address
, 'erc20' AS deposit_token_standard
, 'erc20' AS withdrawal_token_standard
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, CAST(orderId AS varchar) AS bridge_transfer_id
FROM {{ source('butter_' + blockchain, 'butterrouterv2_evt_swapandbridge') }}
LEFT JOIN {{ ref('bridges_butter_chain_indexes') }} m ON d.toChain=m.id

{% endmacro %}