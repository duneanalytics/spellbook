{% macro aero_v1_withdrawals(blockchain) %}

-- Deposits are a persistent message-ID ledger. Do not time-filter this lookup:
-- an old Aero dispatch can settle after the normal incremental lookback.
SELECT origin.chain_id AS deposit_chain_id
    , d.deposit_chain
    , '{{ blockchain }}' AS withdrawal_chain
    , 'Aero' AS bridge_name
    , '1' AS bridge_version
    , l.block_date
    , l.block_time
    , l.block_number
    , d.deposit_amount_raw AS withdrawal_amount_raw
    , d.sender
    , d.recipient
    , destination.token_address AS withdrawal_token_address
    , 'erc20' AS withdrawal_token_standard
    , tx."from" AS tx_from
    , l.tx_hash
    , l.index AS evt_index
    , destination.bridge_address AS contract_address
    , d.bridge_transfer_id
FROM {{ source(blockchain, 'logs') }} l
INNER JOIN {{ source(blockchain, 'transactions') }} tx
    ON tx.hash = l.tx_hash
    AND tx.block_date = l.block_date
    {% if is_incremental() %}
    AND {{ incremental_predicate('tx.block_time') }}
    {% else %}
    AND tx.block_date >= DATE '2026-10-01'
    {% endif %}
INNER JOIN {{ ref('bridges_aero_v1_routes') }} destination
    ON destination.blockchain = '{{ blockchain }}'
    AND l.contract_address = destination.mailbox_address
INNER JOIN {{ ref('bridges_aero_v1_deposits') }} d
    ON d.withdrawal_chain = '{{ blockchain }}'
    AND CAST(l.topic1 AS varchar) = d.bridge_transfer_id
INNER JOIN {{ ref('bridges_aero_v1_routes') }} origin
    ON origin.blockchain = d.deposit_chain
WHERE l.topic0 = keccak(to_utf8('ProcessId(bytes32)'))
    {% if is_incremental() %}
    AND {{ incremental_predicate('l.block_time') }}
    {% else %}
    AND l.block_date >= DATE '2026-10-01'
    {% endif %}
    -- All allowlisted USDC routes use the same decimals and a 1:1 scale.
    -- Confirm the destination token router accepted this exact bridge leg.
    AND EXISTS (
        SELECT 1
        FROM {{ source(blockchain, 'logs') }} received
        WHERE received.block_date = l.block_date
            AND received.tx_hash = l.tx_hash
            AND received.contract_address = destination.bridge_address
            AND received.topic0 = keccak(to_utf8('ReceivedTransferRemote(uint32,bytes32,uint256)'))
            AND varbinary_to_uint256(received.topic1) = origin.chain_id
            AND varbinary_substring(received.topic2, 13, 20) = d.recipient
            AND varbinary_to_uint256(received.data) = d.deposit_amount_raw
            {% if is_incremental() %}
            AND {{ incremental_predicate('received.block_time') }}
            {% else %}
            AND received.block_date >= DATE '2026-10-01'
            {% endif %}
    )

{% endmacro %}
