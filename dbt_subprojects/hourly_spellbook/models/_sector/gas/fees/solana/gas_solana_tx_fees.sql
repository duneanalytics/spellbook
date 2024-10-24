{{ config(
    schema = 'gas_solana',
    alias = 'tx_fees',
    tags = ['prod_exclude'],
    partition_by = ['block_date', 'block_hour'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_date', 'block_slot', 'tx_index']
) }}

WITH base_model AS (
    SELECT
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
    LEFT JOIN {{ ref('solana_compute_limit') }} cl
        ON t.id = cl.tx_id
        AND t.block_date = cl.block_date
        {% if is_incremental() %}
            AND {{ incremental_predicate('cl.block_date') }}
        {% endif %}
    LEFT JOIN {{ ref('solana_compute_unit_price') }} up
        ON t.id = up.tx_id
        AND t.block_date = up.block_date
        {% if is_incremental() %}
            AND {{ incremental_predicate('up.block_date') }}
        {% endif %}
    LEFT JOIN {{ source('solana_utils', 'block_leaders') }} b
        ON t.block_slot = b.slot
        AND t.block_date = b.date
        {% if is_incremental() %}
            AND {{ incremental_predicate('b.date') }}
        {% endif %}
    {% if is_incremental() %}
        WHERE {{ incremental_predicate('t.block_date') }}
    {% endif %}
)

SELECT
    'solana' AS blockchain,
    CAST(date_trunc('month', block_time) AS DATE) AS block_month,
    block_date,
    date_trunc('hour', block_time) AS block_hour,
    block_time,
    block_slot,
    tx_index,
    tx_hash,
    signer,
    compute_unit_price,
    compute_limit,
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
    leader
FROM base_model
LEFT JOIN {{ source('prices','usd_forward_fill') }} p
    ON p.blockchain = 'solana'
    AND p.contract_address = 0x069b8857feab8184fb687f634618c035dac439dc1aeb3b5598a0f00000000001
    AND p.minute = date_trunc('minute', block_time)
    AND date_trunc('day', p.minute) = block_date
    {% if is_incremental() %}
        AND {{ incremental_predicate("date_trunc('day',p.minute)")}}
    {% endif %}
