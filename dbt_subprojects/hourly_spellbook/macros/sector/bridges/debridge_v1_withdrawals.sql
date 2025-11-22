{% macro debridge_v1_withdrawals(blockchain) %}

SELECT t.blockchain AS deposit_chain
, CAST(json_extract_scalar(f."order", '$.takeChainId') AS bigint) AS deposit_chain_id
, '{{blockchain}}' AS withdrawal_chain
, 'deBridge' AS bridge_name
, '1' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, json_extract_scalar(f."order", '$.giveAmount') AS withdrawal_amount_raw
, sender
, json_extract_scalar(f."order", '$.receiverDst') AS recipient
, 'erc20' AS withdrawal_token_standard
, json_extract_scalar(f."order", '$.takeTokenAddress') AS withdrawal_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, CAST(orderId AS varchar) AS bridge_id
FROM {{ source('debridge_' + blockchain, 'dlndestination_evt_fulfilledorder') }} f
LEFT JOIN {{ ref('bridges_debridge_chain_indexes') }} t ON t.id=CAST(json_extract_scalar(f."order", '$.takeChainId') AS bigint)

{% endmacro %}