{% macro zkbridge_v1_withdrawals(blockchain) %}

SELECT i.blockchain AS deposit_chain
, w.srcChainId AS deposit_chain_id
, '{{blockchain}}' AS withdrawal_chain
, 'zkBridge' AS bridge_name
, '1' AS bridge_version
, w.evt_block_date AS block_date
, w.evt_block_time AS block_time
, w.evt_block_number AS block_number
, w.amount AS withdrawal_amount_raw
, CAST(NULL AS varbinary) AS sender
, w.recipient
, t.token_address AS withdrawal_token_address
, CASE WHEN t.token_address=0x0000000000000000000000000000000000000000 THEN 'native' ELSE 'erc20' END AS withdrawal_token_standard
, w.evt_tx_from AS tx_from
, w.evt_tx_hash AS tx_hash
, w.evt_index
, w.contract_address
, CAST(w.poolId AS varchar) + '-' + CAST(w.sequence AS varchar) AS bridge_transfer_id
FROM {{ source('zkbridge_' + blockchain, 'bridge_evt_receivetoken') }} w
LEFT JOIN {{ ref('bridges_zkbridge_chain_indexes') }} i ON i.id=w.srcChainId
LEFT JOIN {{ ref('bridges_zkbridge_token_indexes') }} t ON t.blockchain='{{blockchain}}'
    AND t.contract_address=w.contract_address
    AND t.pool_id=w.poolId

{% endmacro %}