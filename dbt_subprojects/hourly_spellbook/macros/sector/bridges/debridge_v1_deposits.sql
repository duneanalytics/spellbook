{% macro debridge_v1_deposits(blockchain) %}

SELECT '{{blockchain}}' AS deposit_chain
, d.chainIdTo AS withdrawal_chain_id
, m.blockchain AS withdrawal_chain
, 'deBridge' AS bridge_name
, '1' AS bridge_version
, d.evt_block_date AS block_date
, d.evt_block_time AS block_time
, d.evt_block_number AS block_number
, CAST(json_extract_scalar(d.feeParams, '$.receivedAmount') AS UINT256) AS deposit_amount_raw
, d.nativeSender AS sender
, d.receiver AS recipient
, CASE WHEN json_extract_scalar(d.feeParams, '$.isNativeToken') = 'true' THEN 'native' ELSE 'erc20' END AS deposit_token_standard
, t.tokenAddress AS deposit_token_address
, d.evt_tx_from AS tx_from
, d.evt_tx_hash AS tx_hash
, d.evt_index
, d.contract_address
, CAST(d.submissionId AS varchar) AS bridge_transfer_id
FROM {{ source('debridge_' + blockchain, 'debridgegate_evt_sent') }} d
LEFT JOIN {{ source('debridge_' + blockchain, 'debridgegate_evt_pairadded') }} t USING (debridgeId)
LEFT JOIN {{ ref('bridges_debridge_chain_indexes') }} m ON d.chainIdTo=m.id

{% endmacro %}