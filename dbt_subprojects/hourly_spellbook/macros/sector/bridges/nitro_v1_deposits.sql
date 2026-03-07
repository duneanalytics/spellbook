{% macro nitro_v1_deposits(blockchain) %}

SELECT '{{blockchain}}' AS deposit_chain
, try(CAST(replace(from_utf8(d.destChainIdBytes), chr(0), '') AS BIGINT)) AS withdrawal_chain_id
, i.blockchain AS withdrawal_chain
, 'Nitro' AS bridge_name
, '1' AS bridge_version
, d.evt_block_date AS block_date
, d.evt_block_time AS block_time
, d.evt_block_number AS block_number
, d.amount AS deposit_amount_raw
, d.depositor AS sender
, d.recipient
, 'erc20' AS deposit_token_standard
, d.srcToken AS deposit_token_address
, d.evt_tx_from AS tx_from
, d.evt_tx_hash AS tx_hash
, d.evt_index
, d.contract_address
, CAST(d.depositId AS varchar) AS bridge_transfer_id
FROM {{ source('router_' + blockchain, 'assetforwarder_evt_fundsdeposited') }} d
LEFT JOIN {{ source('evms','info') }} i ON i.chain_id=try(CAST(replace(from_utf8(d.destChainIdBytes), chr(0), '') AS BIGINT))

{% endmacro %}