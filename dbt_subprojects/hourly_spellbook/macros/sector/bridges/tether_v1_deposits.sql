{% macro tether_v1_deposits(blockchain) %}

SELECT '{{blockchain}}' AS deposit_chain
, d.dstEid AS withdrawal_chain_id
, i.blockchain AS withdrawal_chain
, 'Tether' AS bridge_name
, '1' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amountSentLD AS deposit_amount_raw
, fromAddress AS sender
, CAST(NULL AS varbinary) AS recipient
, 'erc20' AS deposit_token_standard
, 'erc20' AS withdrawal_token_standard
, t.usdt0_address AS deposit_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, CAST(guid AS varchar) AS bridge_transfer_id
FROM {{ source('tether_' + blockchain, 'oupgradeable_evt_oftsent') }} d
LEFT JOIN {{ ref('bridges_tether_chain_indexes') }} t ON t.blockchain='{{blockchain}}'
LEFT JOIN {{ ref('bridges_tether_chain_indexes') }} i ON d.dstEid=i.id

{% endmacro %}