{{ config(
    schema = 'gas_solana',
    alias = 'compute_limit',
    tags = ['prod_exclude'],
    partition_by = ['block_date', 'block_hour'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_date', 'block_slot', 'tx_id']
) }}

-- this is just decoding program data, could be moved into decoding pipeline

SELECT
    tx_id,
    block_date,
    date_trunc('hour', block_time) AS block_hour,
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
AND inner_instruction_index is null
{% if is_incremental() %}
    AND {{ incremental_predicate('block_date') }}
{% endif %}
