{{ config(
    alias = 'gas_solana_fees',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number', 'tx_hash']
) }}

WITH compute_limit_cte AS (
    SELECT 
        tx_id,
        block_date,
        block_time,
        block_slot,
        bytearray_to_bigint(
            bytearray_reverse(
                bytearray_substring(data, 2, 8)
            )
        ) as compute_limit
    FROM {{ source('solana', 'instruction_calls') }}
    WHERE executing_account = 'ComputeBudget111111111111111111111111111111'
    AND bytearray_substring(data,1,1) = 0x02
),

unit_price_cte AS (
    SELECT 
        tx_id,
        block_date,
        block_time,
        block_slot,
        bytearray_to_bigint(
            bytearray_reverse(
                bytearray_substring(data, 2, 8)
            )
        ) AS compute_unit_price
    FROM {{ source('solana', 'instruction_calls') }}
    WHERE executing_account = 'ComputeBudget111111111111111111111111111111'
),

base_model AS (
    SELECT 
        'normal' as tx_type,
        t.id AS tx_hash,
        t.block_date,
        t.block_slot AS block_number,
        t.block_time,
        t.signer AS tx_from,
        t.fee AS tx_fee_raw,
        (COALESCE(cl.compute_limit, 200000) * COALESCE(up.compute_unit_price, 0) / 1e6) AS prioritization_fee_raw,
        COALESCE(up.compute_unit_price, 0) / 1e6 AS compute_price_lamport,
        COALESCE(cl.compute_limit, 200000) AS compute_limit,
        CASE WHEN cl.compute_limit IS NULL THEN 'default' ELSE 'limit_set' END AS limit_type,
        'So11111111111111111111111111111111111111112' AS tx_fee_currency,
        null AS block_proposer  -- somehow hard to figure out the leader in dune atm
    FROM {{ source('solana', 'transactions') }} t
    LEFT JOIN compute_limit_cte cl 
        ON t.id = cl.tx_id 
        AND t.block_date = cl.block_date  
        {% if is_incremental() %}
            AND {{ incremental_predicate('cl.block_time') }}
        {% endif %}
        {% if not is_incremental() %}
            AND cl.block_time > current_date - interval '2' day
        {% endif %}
    LEFT JOIN unit_price_cte up 
        ON t.id = up.tx_id 
        AND t.block_date = up.block_date
        {% if is_incremental() %}
            AND {{ incremental_predicate('up.block_time') }}
        {% endif %}
        {% if not is_incremental() %}
            AND up.block_time > current_date - interval '2' day
        {% endif %}
    WHERE t.block_date > current_date - interval '2' day
    UNION ALL
    SELECT 
        'vote' as tx_type,
        vt.id AS tx_hash,
        vt.block_date,
        vt.block_slot AS block_number,
        vt.block_time,
        vt.signer AS tx_from,
        vt.fee AS tx_fee_raw,
        null AS prioritization_fee_raw,
        null AS compute_price_lamport,
        null AS compute_limit,
        null AS limit_type,
        'So11111111111111111111111111111111111111112' AS tx_fee_currency,
        null AS block_proposer -- somehow hard to figure out the leader in dune atm
    FROM {{ source('solana', 'vote_transactions') }} vt
    WHERE vt.block_date > current_date - interval '2' day
    )

SELECT
    'solana' AS blockchain,
    CAST(date_trunc('month', block_time) AS DATE) AS block_month,
    block_date,
    block_time,
    block_number,
    tx_hash,
    tx_from,
    NULL AS tx_to, -- this concept doesn't really exist in solana
    compute_price_lamport AS gas_price, -- only applies to compute budget tx
    compute_limit AS gas_used, -- this is the compute limit, not gas
    p.symbol AS currency_symbol,
    tx_fee_raw + coalesce(prioritization_fee_raw,0) AS tx_fee_raw,
    (tx_fee_raw + coalesce(prioritization_fee_raw,0)) / pow(10, 9) AS tx_fee,
    (tx_fee_raw + coalesce(prioritization_fee_raw,0)) / pow(10, 9) * p.price AS tx_fee_usd,
    map(array['base_fee', 'prioritization_fee'], array[tx_fee_raw, prioritization_fee_raw]) AS tx_fee_breakdown_raw,
    transform_values(
        map(array['base_fee', 'prioritization_fee'], array[tx_fee_raw, prioritization_fee_raw]),
        (k, v) -> CAST(v AS double) / pow(10, 9)
    ) AS tx_fee_breakdown,
    transform_values(
        map(array['base_fee', 'prioritization_fee'], array[tx_fee_raw, prioritization_fee_raw]),
        (k, v) -> CAST(v AS double) / pow(10, 9) * p.price
    ) AS tx_fee_breakdown_usd,
    tx_fee_currency,
    block_proposer,
    tx_type,
    limit_type
FROM base_model
LEFT JOIN {{ source('prices', 'usd') }} p
    ON p.blockchain = 'solana'
    --AND to_base58(p.contract_address) = tx_fee_currency  -- this would the right way to do it but slow af
    AND p.contract_address = 0x069b8857feab8184fb687f634618c035dac439dc1aeb3b5598a0f00000000001 --from base58 converted wsol address
    AND p.minute = date_trunc('minute', block_time)
    {% if is_incremental() %}
        AND {{ incremental_predicate('p.minute') }}
    {% endif %}
    {% if not is_incremental() %}
        AND p.minute > current_date - interval '2' day
    {% endif %}
