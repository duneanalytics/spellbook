{{ config(
    schema = 'gas_solana',
    alias = 'compute_limit',
    partition_by = ['block_date', 'block_hour'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_date', 'block_slot', 'tx_id']
) }}

-- this is just decoding program data, could be moved into decoding pipeline
-- version 1 txs can carry several top-level SetComputeUnitLimit instructions; keep the first.
-- picked per row from the ordered instructions array, since a window/aggregate over full history exceeds memory

WITH first_compute_limit_instruction AS (
SELECT
    id AS tx_id,
    block_date,
    date_trunc('hour', block_time) AS block_hour,
    block_time,
    block_slot,
    index AS tx_index,
    element_at(
        filter(
            instructions,
            x -> x.executing_account = 'ComputeBudget111111111111111111111111111111'
                AND bytearray_substring(from_base58(x.data), 1, 1) = 0x02
        ),
        1
    ) AS instruction
FROM {{ source('solana', 'transactions') }}
{% if is_incremental() %}
WHERE {{ incremental_predicate('block_date') }}
{% endif %}
)

SELECT
    tx_id,
    block_date,
    block_hour,
    block_time,
    block_slot,
    tx_index,
    bytearray_to_bigint(
        bytearray_reverse(
            bytearray_substring(from_base58(instruction.data), 2, 8)
        )
    ) as compute_limit
FROM first_compute_limit_instruction
WHERE instruction IS NOT NULL
