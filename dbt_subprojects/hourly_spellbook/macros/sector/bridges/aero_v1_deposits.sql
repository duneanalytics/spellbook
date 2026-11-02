{% macro aero_v1_deposits(blockchain) %}

-- transferRemote is called by Aero's Metarouter, but the USDC route is shared.
-- Require its returned message ID to have a persisted Mailbox dispatch log:
-- successful child traces inside reverted batches must never become deposits.
SELECT '{{ blockchain }}' AS deposit_chain
    , destination.chain_id AS withdrawal_chain_id
    , destination.blockchain AS withdrawal_chain
    , 'Aero' AS bridge_name
    , '1' AS bridge_version
    , t.block_date
    , t.block_time
    , t.block_number
    , varbinary_to_uint256(varbinary_substring(t.input, 69, 32)) AS deposit_amount_raw
    , t."from" AS sender
    , varbinary_substring(t.input, 49, 20) AS recipient
    , origin.token_address AS deposit_token_address
    , 'erc20' AS deposit_token_standard
    , aero.call_tx_from AS tx_from
    , t.tx_hash
    , l.index AS evt_index
    , t."to" AS contract_address
    , CAST(t.output AS varchar) AS bridge_transfer_id
FROM {{ source(blockchain, 'traces') }} t
INNER JOIN (
    SELECT DISTINCT call_tx_hash, call_block_date, call_tx_from
    FROM {{ source('aero_' + blockchain, 'metarouter_call_execute') }}
    WHERE contract_address = 0x0fEf9e88A7b76BCDBF451780C6774FE32965Ee98
        AND call_success
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
        {% else %}
        AND call_block_date >= DATE '2026-10-01'
        {% endif %}
) aero
    ON aero.call_tx_hash = t.tx_hash
    AND aero.call_block_date = t.block_date
INNER JOIN {{ ref('bridges_aero_v1_routes') }} origin
    ON origin.blockchain = '{{ blockchain }}'
    AND t."to" = origin.bridge_address
INNER JOIN {{ ref('bridges_aero_v1_routes') }} destination
    ON varbinary_to_uint256(varbinary_substring(t.input, 5, 32)) = destination.chain_id
    AND destination.blockchain <> origin.blockchain
INNER JOIN {{ source(blockchain, 'logs') }} l
    ON l.tx_hash = t.tx_hash
    AND l.block_date = t.block_date
    AND l.contract_address = origin.mailbox_address
    AND l.topic0 = keccak(to_utf8('DispatchId(bytes32)'))
    AND l.topic1 = t.output
    {% if is_incremental() %}
    AND {{ incremental_predicate('l.block_time') }}
    {% else %}
    AND l.block_date >= DATE '2026-10-01'
    {% endif %}
WHERE t."from" = 0x0fEf9e88A7b76BCDBF451780C6774FE32965Ee98
    AND t.call_type = 'call'
    AND t.success
    AND t.tx_success
    AND length(t.input) = 100
    AND length(t.output) = 32
    AND varbinary_substring(t.input, 1, 4) = varbinary_substring(keccak(to_utf8('transferRemote(uint32,bytes32,uint256)')), 1, 4)
    {% if is_incremental() %}
    AND {{ incremental_predicate('t.block_time') }}
    {% else %}
    AND t.block_date >= DATE '2026-10-01'
    {% endif %}

{% endmacro %}
