{{ config(
    schema = 'gas_solana',
    alias = 'compute_limit',
    materialized = 'view'
) }}

-- Backward-compat view. Physical decoding lives in gas_solana_compute_budget which
-- also holds compute_unit_price. Filter compute_limit IS NOT NULL to preserve the
-- historical row set (only txs that emitted the SetComputeUnitLimit opcode 0x02).

SELECT
    tx_id,
    block_date,
    block_hour,
    block_time,
    block_slot,
    tx_index,
    compute_limit
FROM {{ ref('gas_solana_compute_budget') }}
WHERE compute_limit IS NOT NULL
