{% macro butter_v2_deposits(blockchain) %}

SELECT '{{blockchain}}' AS deposit_chain
, d.toChain AS withdrawal_chain_id
, m.blockchain AS withdrawal_chain
, 'Butter' AS bridge_name
, '2.1' AS bridge_version
, d.evt_block_date AS block_date
, d.evt_block_time AS block_time
, d.evt_block_number AS block_number
, d.originAmount AS deposit_amount_raw
, d.bridgeAmount AS withdrawal_amount_raw
, d."from" AS sender
, d.to AS recipient
, d.originToken AS deposit_token_address
, d.bridgeToken AS withdrawal_token_address
, 'erc20' AS deposit_token_standard
, 'erc20' AS withdrawal_token_standard
, d.evt_tx_from AS tx_from
, d.evt_tx_hash AS tx_hash
, d.evt_index
, d.contract_address
, CAST(d.orderId AS varchar) AS bridge_transfer_id
FROM {{ source('butter_' + blockchain, 'butterrouterv2_evt_swapandbridge') }} d
LEFT JOIN {{ ref('bridges_butter_chain_indexes') }} m ON d.toChain=m.id

{% endmacro %}