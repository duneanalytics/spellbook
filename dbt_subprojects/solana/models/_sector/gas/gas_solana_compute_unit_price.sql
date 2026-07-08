{{ config(
    schema = 'gas_solana',
    alias = 'compute_unit_price',
    materialized = 'view'
) }}

-- Backward-compat view. Physical decoding lives in gas_solana_compute_budget which
-- also holds compute_limit. Filter compute_unit_price IS NOT NULL to preserve the
-- historical row set (only txs that emitted the SetComputeUnitPrice opcode 0x03).

SELECT
    tx_id,
    block_date,
    block_hour,
    block_time,
    block_slot,
    tx_index,
    compute_unit_price
FROM {{ ref('gas_solana_compute_budget') }}
WHERE compute_unit_price IS NOT NULL
