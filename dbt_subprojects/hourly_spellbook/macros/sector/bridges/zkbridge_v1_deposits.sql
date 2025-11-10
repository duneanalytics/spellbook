{% macro zkbridge_v1_deposits(blockchain) %}

SELECT '{{blockchain}}' AS deposit_chain
, d.dstChainId AS withdrawal_chain_id
, i.blockchain AS withdrawal_chain
, 'zkBridge' AS bridge_name
, '1' AS bridge_version
, d.evt_block_date AS block_date
, d.evt_block_time AS block_time
, d.evt_block_number AS block_number
, d.amount AS deposit_amount_raw
, d.sender
, d.recipient
, t.token_address AS deposit_token_address
, CASE WHEN t.token_address=0x0000000000000000000000000000000000000000 THEN 'native' ELSE 'erc20' END AS deposit_token_standard
, d.evt_tx_from AS tx_from
, d.evt_tx_hash AS tx_hash
, d.evt_index
, d.contract_address
, CAST(d.poolId AS varchar) + '-' + CAST(d.sequence AS varchar) AS bridge_transfer_id
FROM {{ source('zkbridge_' + blockchain, 'bridge_evt_transfertoken') }} d
LEFT JOIN {{ ref('bridges_zkbridge_chain_indexes') }} i ON i.id=d.dstChainId
LEFT JOIN {{ ref('bridges_zkbridge_token_indexes') }} t ON t.blockchain='{{blockchain}}'
    AND t.contract_address=d.contract_address
    AND t.pool_id=d.poolId

{% endmacro %}