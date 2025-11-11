{% macro layerzero_v1_deposits(blockchain, events) %}

, send_calls AS (
    SELECT s._dstChainId AS withdrawal_chain_id
    , date_trunc('day', s.call_block_time) AS block_date
    , s.call_block_time AS block_time
    , s.call_block_number AS block_number
    , s.call_tx_from AS sender
    , s._refundAddress AS recipient
    , s.contract_address AS deposit_token_address
    , s.call_tx_from AS tx_from
    , s.call_tx_hash AS tx_hash
    , s.contract_address
    , ROW_NUMBER() OVER(PARTITION BY s.call_block_number, s.call_tx_hash ORDER BY s.call_trace_address ASC) AS call_send_index
    FROM {{ events }} s
    WHERE s.call_success
    )

SELECT '{{blockchain}}' AS deposit_chain
, sc.withdrawal_chain_id
, ci.blockchain AS withdrawal_chain
, 'LayerZero' AS bridge_name
, '1' AS bridge_version
, date_trunc('day', sc.block_time) AS block_date
, sc.block_time
, sc.block_number
, t.amount AS deposit_amount_raw
, sc.sender
, sc.recipient
, t.token_standard AS deposit_token_standard
, sc.deposit_token_address
, sc.tx_from
, sc.tx_hash
, t.evt_index AS evt_index
, sc.contract_address
, t.unique_key AS bridge_transfer_id
FROM send_calls sc
LEFT JOIN {{ source('tokens_' + blockchain, 'transfers') }} t ON t.block_number=sc.call_block_number
        AND t.tx_hash=sc.call_tx_hash
LEFT JOIN {{ ref('bridges_layerzero_chain_indexes') }} ci ON sc.withdrawal_chain_id=ci.chain_id

{% endmacro %}