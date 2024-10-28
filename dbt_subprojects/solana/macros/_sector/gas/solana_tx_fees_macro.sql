{% macro solana_tx_fees_macro(start_date, end_date) %}
WITH base_model AS (
    SELECT
        'transaction' as tx_type,
        t.id AS tx_hash,
        t.block_date,
        t.block_slot,
        t.index as tx_index,
        t.block_time,
        t.signer,
        t.fee AS tx_fee_raw,
        5000*required_signatures as base_fee_raw, -- each signature is 5000 lamports
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
        {% elif not is_incremental() %}
            AND cl.block_date >= {{ start_date }}
            AND cl.block_date < {{ end_date }}
        {% endif %}
    LEFT JOIN {{ ref('solana_compute_unit_price') }} up
        ON t.id = up.tx_id
        AND t.block_date = up.block_date
        {% if is_incremental() %}
            AND {{ incremental_predicate('up.block_date') }}
        {% elif not is_incremental() %}
            AND up.block_date >= {{ start_date }}
            AND up.block_date < {{ end_date }}
        {% endif %}
    LEFT JOIN {{ source('solana_utils', 'block_leaders') }} b
        ON t.block_slot = b.slot
        AND t.block_date = b.date
        {% if is_incremental() %}
            AND {{ incremental_predicate('b.date') }}
        {% elif not is_incremental() %}
            AND b.date >= {{ start_date }}
            AND b.date < {{ end_date }}
        {% endif %}
    WHERE 1=1
    {% if is_incremental() %}
        AND {{ incremental_predicate('t.block_date') }}
    {% elif not is_incremental() %}
        AND t.block_date >= {{ start_date }}
        AND t.block_date < {{ end_date }}
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
    tx_fee_raw,
    tx_fee_raw / pow(10, 9) AS tx_fee,
    tx_fee_raw / pow(10, 9) * p.price AS tx_fee_usd,
    map(array['base_fee', 'prioritization_fee'], array[coalesce(base_fee_raw, 0), coalesce(prioritization_fee_raw, 0)]) AS tx_fee_breakdown_raw,
    transform_values(
        map(array['base_fee', 'prioritization_fee'], array[coalesce(base_fee_raw, 0), coalesce(prioritization_fee_raw, 0)]),
        (k, v) -> CAST(v AS double) / pow(10, 9)
    ) AS tx_fee_breakdown,
    transform_values(
        map(array['base_fee', 'prioritization_fee'], array[coalesce(base_fee_raw, 0), coalesce(prioritization_fee_raw, 0)]),
        (k, v) -> CAST(v AS double) / pow(10, 9) * p.price
    ) AS tx_fee_breakdown_usd,
    tx_fee_currency,
    leader,
    tx_type
FROM base_model
LEFT JOIN {{ source('prices','usd_forward_fill') }} p
    ON p.blockchain = 'solana'
    AND p.contract_address = 0x069b8857feab8184fb687f634618c035dac439dc1aeb3b5598a0f00000000001
    AND p.minute = date_trunc('minute', block_time)
    AND date_trunc('day', p.minute) = block_date
    {% if is_incremental() %}
        AND {{ incremental_predicate('p.minute') }}
    {% elif not is_incremental() %}
        AND p.minute >= {{ start_date }}
        AND p.minute < {{ end_date }}
    {% endif %}
{% endmacro %}
