{{ config(
    schema = 'gas_solana',
    alias = 'fees',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_date', 'block_slot', 'tx_index']
) }}

WITH compute_limit_cte AS (
    SELECT
        tx_id,
        block_date,
        block_time,
        block_slot,
        tx_index,
        bytearray_to_bigint(
            bytearray_reverse(
                bytearray_substring(data, 2, 8)
            )
        ) as compute_limit
    FROM {{ source('solana', 'instruction_calls') }}
    WHERE executing_account = 'ComputeBudget111111111111111111111111111111'
    AND bytearray_substring(data,1,1) = 0x02
    AND inner_instruction_index is null -- compute budget and price are inherited on cross program invocation
    {% if is_incremental() or true %}
            AND {{ incremental_predicate('block_time') }}
    {% endif %}
),

unit_price_cte AS (
    SELECT
        tx_id,
        block_date,
        block_time,
        block_slot,
        tx_index,
        bytearray_to_bigint(
            bytearray_reverse(
                bytearray_substring(data, 2, 8)
            )
        ) AS compute_unit_price
    FROM {{ source('solana', 'instruction_calls') }}
    WHERE executing_account = 'ComputeBudget111111111111111111111111111111'
    AND bytearray_substring(data,1,1) = 0x03
    AND inner_instruction_index is null -- compute budget and price are inherited on cross program invocation
    {% if is_incremental() or true %}
            AND {{ incremental_predicate('block_time') }}
    {% endif %}
),

base_model AS (
    SELECT
        'normal' as tx_type,
        t.id AS tx_hash,
        t.block_date,
        t.block_slot,
        t.index as tx_index,
        t.block_time,
        t.signer,
        t.fee AS tx_fee_raw,
        (COALESCE(cl.compute_limit, 200000) * COALESCE(up.compute_unit_price/ 1e6, 0)) AS prioritization_fee_raw,
        COALESCE(up.compute_unit_price/ 1e6, 0) AS compute_unit_price,
        COALESCE(cl.compute_limit, 200000) AS compute_limit,
        'So11111111111111111111111111111111111111112' AS tx_fee_currency,
        b.leader
    FROM {{ source('solana', 'transactions') }} t
    LEFT JOIN compute_limit_cte cl
        ON t.id = cl.tx_id
        AND t.block_slot = cl.block_slot
    LEFT JOIN unit_price_cte up
        ON t.id = up.tx_id
        AND t.block_slot = up.block_slot
    LEFT JOIN {{ source('solana_utils', 'block_leaders') }} b
        ON t.block_slot = b.slot
        AND t.block_date = b.date
        {% if is_incremental() or true %}
            AND {{ incremental_predicate('b.time') }}
        {% endif %}
    {% if is_incremental() or true %}
            WHERE {{ incremental_predicate('t.block_time') }}
    {% endif %}
    UNION ALL
    SELECT
        'vote' as tx_type,
        vt.id AS tx_hash,
        vt.block_date,
        vt.block_slot,
        vt.index as tx_index,
        vt.block_time,
        vt.signer,
        vt.fee AS tx_fee_raw,
        null AS prioritization_fee_raw,
        null AS compute_price_lamport,
        null AS compute_limit,
        'So11111111111111111111111111111111111111112' AS tx_fee_currency,
        b.leader
    FROM {{ source('solana', 'vote_transactions') }} vt
    LEFT JOIN {{ source('solana_utils', 'block_leaders') }} b
        ON vt.block_slot = b.slot
        AND vt.block_date = b.date
        {% if is_incremental() or true %}
            AND {{ incremental_predicate('b.time') }}
        {% endif %}
    {% if is_incremental() or true %}
    WHERE {{ incremental_predicate('vt.block_time') }}
    {% endif %}
)

SELECT
    'solana' AS blockchain,
    CAST(date_trunc('month', block_time) AS DATE) AS block_month,
    block_date,
    block_time,
    block_slot,
    tx_index,
    tx_hash,
    signer,
    --NULL AS tx_to, -- this concept doesn't really exist in solana
    compute_unit_price, -- only applies to compute budget tx
    compute_limit, -- this is the compute limit, not gas
    p.symbol AS currency_symbol,
    tx_fee_raw + coalesce(prioritization_fee_raw,0) AS tx_fee_raw,
    (tx_fee_raw + coalesce(prioritization_fee_raw,0)) / pow(10, 9) AS tx_fee,
    (tx_fee_raw + coalesce(prioritization_fee_raw,0)) / pow(10, 9) * p.price AS tx_fee_usd,
    map(array['base_fee', 'prioritization_fee'], array[coalesce(tx_fee_raw, 0), coalesce(prioritization_fee_raw, 0)]) AS tx_fee_breakdown_raw,
    transform_values(
        map(array['base_fee', 'prioritization_fee'], array[coalesce(tx_fee_raw, 0), coalesce(prioritization_fee_raw, 0)]),
        (k, v) -> CAST(v AS double) / pow(10, 9)
    ) AS tx_fee_breakdown,
    transform_values(
        map(array['base_fee', 'prioritization_fee'], array[coalesce(tx_fee_raw, 0), coalesce(prioritization_fee_raw, 0)]),
        (k, v) -> CAST(v AS double) / pow(10, 9) * p.price
    ) AS tx_fee_breakdown_usd,
    tx_fee_currency,
    leader,
    tx_type
FROM base_model
LEFT JOIN {{ source('prices','usd_forward_fill') }} p
    ON p.blockchain = 'solana'
    --AND to_base58(p.contract_address) = tx_fee_currency  -- this would the right way to do it but slow af
    AND p.contract_address = 0x069b8857feab8184fb687f634618c035dac439dc1aeb3b5598a0f00000000001 --from base58 converted wsol address
    AND p.minute = date_trunc('minute', block_time)
    {% if is_incremental() or true %}
        AND {{ incremental_predicate('p.minute') }}
    {% endif %}
WHERE 1=1
{% if is_incremental() %}
--    and block_time > (select max(block_time) from {{this}})
{% endif %}
-- run 1h behind to allow for late data
and block_time < now() - interval '1' hour
