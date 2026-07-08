{{ config(
    schema = 'gas_solana',
    alias = 'compute_budget',
    partition_by = ['block_date', 'block_hour'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_date', 'block_slot', 'tx_id']
) }}

-- Merges the two Solana ComputeBudget decodings into a single (block_date, tx_id)-keyed
-- model: SetComputeUnitLimit (opcode 0x02) and SetComputeUnitPrice (opcode 0x03). This
-- scans solana.instruction_calls once with an IN filter instead of two separate scans
-- and gives downstream consumers a single join key. gas_solana_compute_limit and
-- gas_solana_compute_unit_price are kept as views over this table for backward compat.

WITH raw AS (
    SELECT
        tx_id,
        block_date,
        date_trunc('hour', block_time) AS block_hour,
        block_time,
        block_slot,
        tx_index,
        bytearray_substring(data, 1, 1) AS op,
        bytearray_to_bigint(
            bytearray_reverse(
                bytearray_substring(data, 2, 8)
            )
        ) AS val
    FROM {{ source('solana', 'instruction_calls') }}
    WHERE executing_account = 'ComputeBudget111111111111111111111111111111'
      AND executing_account_prefix = 'Co'
      AND bytearray_substring(data, 1, 1) IN (0x02, 0x03)
      AND inner_instruction_index IS NULL
    {% if is_incremental() %}
      AND {{ incremental_predicate('block_date') }}
    {% endif %}
)

SELECT
    tx_id,
    block_date,
    block_hour,
    block_time,
    block_slot,
    tx_index,
    MAX(CASE WHEN op = 0x02 THEN val END) AS compute_limit,
    MAX(CASE WHEN op = 0x03 THEN val END) AS compute_unit_price
FROM raw
GROUP BY tx_id, block_date, block_hour, block_time, block_slot, tx_index
