{% macro zkbridge_v1_withdrawals(blockchain) %}

WITH bridge_transfer_events AS (
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
    , CAST(w.poolId AS varchar) || '-' || CAST(w.sequence AS varchar) AS bridge_transfer_id
    , ROW_NUMBER() OVER (PARTITION BY w.srcChainId, w.poolId, w.sequence ORDER BY w.evt_block_number, w.evt_index) AS rn
    FROM {{ source('zkbridge_' + blockchain, 'bridge_evt_receivetoken') }} w
    LEFT JOIN {{ ref('bridges_zkbridge_chain_indexes') }} i ON i.id=w.srcChainId
    LEFT JOIN {{ ref('bridges_zkbridge_token_indexes') }} t ON t.blockchain='{{blockchain}}'
        AND t.contract_address=w.contract_address
        AND t.pool_id=w.poolId
    )

{% if is_incremental() %}
, deduped AS (
    SELECT w.deposit_chain
    , w.deposit_chain_id
    , w.withdrawal_chain
    , w.bridge_name
    , w.bridge_version
    , w.block_date
    , w.block_time
    , w.block_number
    , w.withdrawal_amount_raw
    , w.sender
    , w.recipient
    , w.withdrawal_token_address
    , w.withdrawal_token_standard
    , w.tx_from
    , w.tx_hash
    , w.evt_index
    , w.contract_address
    , w.bridge_transfer_id
    , w.rn + MAX(CAST(regexp_replace(t.bridge_transfer_id, '.*-', '') AS int)) AS rn
    FROM bridge_transfer_events w
    INNER JOIN {{this}} t ON t.bridge_name = 'zkBridge'
        AND t.bridge_version = '1'
        AND t.deposit_chain_id = w.srcChainId
        AND regexp_replace(t.bridge_transfer_id, '-[^-]*$', '') = CAST(w.poolId AS varchar) || '-' || CAST(w.sequence AS varchar)
    GROUP BY w.deposit_chain, w.deposit_chain_id, w.withdrawal_chain, w.bridge_name, w.bridge_version, w.block_date, w.block_time, w.block_number, w.withdrawal_amount_raw, w.sender, w.recipient, d.withdrawal_token_address, d.withdrawal_token_standard, d.tx_from, d.tx_hash, d.evt_index, d.contract_address, w.bridge_transfer_id, w.rn
    )
{% endif %}

SELECT deposit_chain
, deposit_chain_id
, withdrawal_chain
, bridge_name
, bridge_version
, block_date
, block_time
, block_number
, withdrawal_amount_raw
, sender
, recipient
, withdrawal_token_address
, withdrawal_token_standard
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