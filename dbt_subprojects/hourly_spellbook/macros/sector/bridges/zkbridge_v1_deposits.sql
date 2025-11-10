{% macro zkbridge_v1_deposits(blockchain) %}

WITH bridge_transfer_events AS (
    SELECT DISTINCT '{{blockchain}}' AS deposit_chain
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
    , CAST(d.poolId AS varchar) || '-' || CAST(d.sequence AS varchar) AS bridge_transfer_id
    , ROW_NUMBER() OVER (PARTITION BY d.dstChainId, d.poolId, d.sequence ORDER BY d.evt_block_number, d.evt_index) AS rn
    FROM {{ source('zkbridge_' + blockchain, 'bridge_evt_transfertoken') }} d
    LEFT JOIN {{ ref('bridges_zkbridge_chain_indexes') }} i ON i.id=d.dstChainId
    LEFT JOIN {{ ref('bridges_zkbridge_token_indexes') }} t ON t.blockchain='{{blockchain}}'
        AND t.contract_address=d.contract_address
        AND t.pool_id=d.poolId
    )

{% if is_incremental() %}
, deduped AS (
    SELECT d.deposit_chain
    , d.withdrawal_chain_id
    , d.withdrawal_chain
    , d.bridge_name
    , d.bridge_version
    , d.block_date
    , d.block_time
    , d.block_number
    , d.deposit_amount_raw
    , d.sender
    , d.recipient
    , d.deposit_token_address
    , d.deposit_token_standard
    , d.tx_from
    , d.tx_hash
    , d.evt_index
    , d.contract_address
    , d.bridge_transfer_id
    , d.rn + MAX(CAST(regexp_replace(t.bridge_transfer_id, '.*-', '') AS int)) AS rn
    FROM bridge_transfer_events d
    INNER JOIN {{this}} t ON t.bridge_name = 'zkBridge'
        AND t.bridge_version = '1'
        AND t.withdrawal_chain_id = d.dstChainId
        AND regexp_replace(t.bridge_transfer_id, '-[^-]*$', '') = CAST(d.poolId AS varchar) || '-' || CAST(d.sequence AS varchar)
    GROUP BY d.deposit_chain, d.withdrawal_chain_id, d.withdrawal_chain, d.bridge_name, d.bridge_version, d.block_date, d.block_time, d.block_number, d.deposit_amount_raw, d.sender, d.recipient, d.deposit_token_address, d.deposit_token_standard, d.tx_from, d.tx_hash, d.evt_index, d.contract_address, d.bridge_transfer_id, d.rn
    )
{% endif %}

SELECT deposit_chain
, withdrawal_chain_id
, withdrawal_chain
, bridge_name
, bridge_version
, block_date
, block_time
, block_number
, deposit_amount_raw
, sender
, recipient
, deposit_token_address
, deposit_token_standard
, tx_from
, tx_hash
, evt_index
, contract_address
, bridge_transfer_id || '-' || CAST(rn AS varchar) AS bridge_transfer_id
{% if is_incremental() %}
FROM deduped
{% else %}
FROM bridge_transfer_events
{% endif %}

{% endmacro %}