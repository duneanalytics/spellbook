{% macro connext_v1_deposits(blockchain) %}

SELECT '{{blockchain}}' AS deposit_chain
, CAST(json_extract_scalar(params, '$.destinationDomain') AS integer) AS withdrawal_chain_id
, i.blockchain AS withdrawal_chain
, 'Connext' AS bridge_name
, '1' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, CAST(json_extract_scalar(params, '$.bridgeAmt') AS double) AS deposit_amount_raw
, from_hex(json_extract_scalar(params, '$.originSender')) AS sender
, from_hex(json_extract_scalar(params, '$.to')) AS recipient
, CASE WHEN local=0x0000000000000000000000000000000000000000 THEN 'native' ELSE 'erc20' END AS deposit_token_standard
, local AS deposit_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, CAST(transferId AS varchar) AS bridge_transfer_id
FROM {{ source('connext_' + blockchain, 'connextdiamond_evt_xcalled') }} d
LEFT JOIN {{ ref('bridges_connext_chain_indexes') }} i ON CAST(json_extract_scalar(params, '$.destinationDomain') AS integer)=i.id

{% endmacro %}