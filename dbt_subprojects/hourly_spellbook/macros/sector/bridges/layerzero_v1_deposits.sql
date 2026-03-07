{% macro layerzero_v1_deposits(blockchain, events) %}

WITH send_calls AS (
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
    , ROW_NUMBER() OVER(PARTITION BY s.call_tx_hash ORDER BY s.call_trace_address ASC) AS call_send_index
    FROM {{ events }} s
    WHERE s.call_success
    )

, distinct_calls AS (
    SELECT block_number
    , tx_hash
    , MAX(call_send_index) AS max_call_send_index
    FROM send_calls
    GROUP BY 1, 2
    )

, transfers AS (
    SELECT block_number
    , tx_hash
    , sender
    , recipient
    , deposit_amount_raw
    , deposit_token_standard
    , deposit_token_address
    , evt_index
    , unique_key
    , rn
    FROM (
        SELECT t.block_number
        , t.tx_hash
        , t."from" AS sender
        , t.to AS recipient
        , t.amount AS deposit_amount_raw
        , t.token_standard AS deposit_token_standard
        , t.contract_address AS deposit_token_address
        , t.evt_index
        , t.unique_key
        , ROW_NUMBER() OVER (PARTITION BY t.tx_hash ORDER BY COALESCE(t.trace_address, ARRAY[t.evt_index])) AS rn
        , sc.max_call_send_index
        FROM {{ source('tokens_' + blockchain, 'transfers') }} t
        INNER JOIN {{ ref('bridges_layerzero_chain_indexes') }} i ON i.blockchain='{{blockchain}}'
        INNER JOIN distinct_calls sc ON t.block_number=sc.block_number
                AND t.tx_hash=sc.tx_hash
                AND t.to=i.endpoint_address
        )
    WHERE rn <= max_call_send_index
    )

SELECT distinct '{{blockchain}}' AS deposit_chain
, sc.withdrawal_chain_id
, ci.blockchain AS withdrawal_chain
, 'LayerZero' AS bridge_name
, '1' AS bridge_version
, date_trunc('day', sc.block_time) AS block_date
, sc.block_time
, sc.block_number
, t.deposit_amount_raw
, sc.sender
, sc.recipient
, t.deposit_token_standard
, t.deposit_token_address
, sc.tx_from
, sc.tx_hash
, COALESCE(t.evt_index, -sc.call_send_index) AS evt_index
, sc.contract_address
, {{ dbt_utils.generate_surrogate_key(['sc.tx_hash', 't.evt_index', 'sc.call_send_index']) }} as bridge_transfer_id
FROM send_calls sc
LEFT JOIN transfers t ON t.block_number=sc.block_number
        AND t.tx_hash=sc.tx_hash
        AND t.rn=sc.call_send_index
LEFT JOIN {{ ref('bridges_layerzero_chain_indexes') }} ci ON sc.withdrawal_chain_id=ci.id

{% endmacro %}