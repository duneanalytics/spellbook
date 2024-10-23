{{ config(
    schema = 'gas_solana',
    alias = 'vote_fees',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_date', 'block_slot', 'tx_id']
) }}

SELECT
    'vote' as tx_type,
    vt.id AS tx_hash,
    vt.block_date,
    vt.block_slot,
    vt.index as tx_index,
    vt.block_time,
    vt.signer,
    vt.fee AS tx_fee_raw,
    CAST(null AS bigint) AS prioritization_fee_raw,
    CAST(null AS double) AS compute_unit_price,
    CAST(null AS bigint) AS compute_limit,
    'So11111111111111111111111111111111111111112' AS tx_fee_currency,
    b.leader
FROM {{ source('solana', 'vote_transactions') }} vt
LEFT JOIN {{ source('solana_utils', 'block_leaders') }} b
    ON vt.block_slot = b.slot
    AND vt.block_date = b.date
    {% if is_incremental() %}
        AND {{ incremental_predicate('b.date') }}
    {% endif %}
{% if is_incremental() %}
WHERE {{ incremental_predicate('vt.block_date') }}
{% endif %}
