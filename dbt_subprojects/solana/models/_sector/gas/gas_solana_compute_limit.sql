{{ config(
    schema = 'gas_solana',
    alias = 'compute_limit',
    partition_by = ['block_date', 'block_hour'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_date', 'block_slot', 'tx_id']
) }}

-- Read the effective compute limit from the transaction config. Version 1
-- transactions carry this config in the message; their legacy compute-budget
-- instructions can be no-ops and may not represent the effective limit.

SELECT
    id AS tx_id,
    block_date,
    date_trunc('hour', block_time) AS block_hour,
    block_time,
    block_slot,
    index AS tx_index,
    transaction_config.compute_unit_limit AS compute_limit
FROM {{ source('solana', 'transactions') }}
WHERE transaction_config.compute_unit_limit IS NOT NULL
{% if is_incremental() %}
    AND {{ incremental_predicate('block_date') }}
{% endif %}
