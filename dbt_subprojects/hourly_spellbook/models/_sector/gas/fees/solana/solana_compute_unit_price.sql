{{ config(
    schema = 'gas_solana',
    alias = 'compute_unit_price',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_date', 'block_slot', 'tx_id']
) }}

-- this is just decoding program data, could be moved into decoding pipeline

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
AND inner_instruction_index is null
{% if is_incremental() %}
    AND {{ incremental_predicate('block_date') }}
{% endif %}
