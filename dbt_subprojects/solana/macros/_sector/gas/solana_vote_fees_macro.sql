{% macro solana_vote_fees_macro(start_date, end_date) %}

WITH base_model AS (
    SELECT
        'vote' as tx_type,
        vt.id AS tx_hash,
        vt.block_date,
        vt.block_slot,
        vt.index as tx_index,
        vt.block_time,
        vt.signer,
        vt.fee AS tx_fee_raw,
        vt.fee as base_fee_raw,
        CAST(null AS bigint) AS prioritization_fee_raw,
        CAST(null AS double) AS compute_unit_price,
        CAST(null AS bigint) AS compute_limit,
        'So11111111111111111111111111111111111111112' AS tx_fee_currency,
        b.leader
    FROM {{ source('solana', 'vote_transactions') }} vt
    LEFT JOIN {{ ref('solana_utils_block_leaders') }} b 
        ON vt.block_slot = b.slot
        AND vt.block_date = b.date
        {% if is_incremental() %}
        AND {{ incremental_predicate('b.date') }}
        {% else %}
        AND b.date >= {{ start_date }}
        AND b.date < date_add('day', 1, {{ start_date }})
        {% endif %}
    WHERE 1=1    
        {% if is_incremental() %}
        AND {{ incremental_predicate('vt.block_date') }}
        {% else %}
        AND vt.block_date >= {{ start_date }}
        AND vt.block_date < date_add('day', 1, {{ start_date }})
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
    {% else %}
    AND p.minute >= {{ start_date }}
    AND p.minute < date_add('day', 1, {{ start_date }})
    {% endif %}
   
{% endmacro %}