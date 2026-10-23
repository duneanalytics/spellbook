{{ config(
    schema = 'gas_solana',
    alias = 'compute_unit_price',
    partition_by = ['block_date', 'block_hour'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_date', 'block_slot', 'tx_id']
) }}

-- this is just decoding program data, could be moved into decoding pipeline
-- version 1 txs can carry several top-level SetComputeUnitPrice instructions; keep the first.
-- aggregate the prefix-pruned instruction_calls scan; ranking all solana.transactions exceeded memory

SELECT
    tx_id,
    block_date,
    date_trunc('hour', min(block_time)) AS block_hour,
    min(block_time) AS block_time,
    block_slot,
    min(tx_index) AS tx_index,
    min_by(
        bytearray_to_bigint(
            bytearray_reverse(
                bytearray_substring(data, 2, 8)
            )
        ),
        outer_instruction_index
    ) AS compute_unit_price
FROM {{ source('solana', 'instruction_calls') }}
WHERE executing_account = 'ComputeBudget111111111111111111111111111111'
AND executing_account_prefix = 'Co'
AND bytearray_substring(data, 1, 1) = 0x03
AND inner_instruction_index is null
{% if is_incremental() %}
AND {{ incremental_predicate('block_date') }}
{% endif %}
{% if target.name == 'ci' %}
-- full refresh in CI has no incremental predicate; prod incremental stays on the 1-day window
AND block_date >= current_date - interval '3' day
{% endif %}
GROUP BY tx_id, block_date, block_slot
