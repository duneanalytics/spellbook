{{
  config(
    materialized='incremental',
    unique_key=['block_date', 'program_id', 'tx_id'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
  )
}}

WITH compute_units_data AS (
  SELECT
    DATE_TRUNC('day', block_time) as block_date,
    block_time,
    block_slot,
    tx_id,
    executing_account as program_id,
    COUNT(*) as instruction_count,
    SUM(CASE WHEN tx_success THEN 1 ELSE 0 END) as successful_instructions,
    SUM(CASE WHEN NOT tx_success THEN 1 ELSE 0 END) as failed_instructions
  FROM {{ source('solana', 'instruction_calls') }}
  WHERE block_time >= '2021-01-01' -- Adjust based on your data availability
  {% if is_incremental() %}
    AND block_date >= (SELECT MAX(block_date) FROM {{ this }})
  {% endif %}
  GROUP BY 1, 2, 3, 4, 5
),

transaction_metadata AS (
  SELECT
    tx_id,
    fee,
    success as tx_success,
    block_time,
    block_slot
  FROM {{ source('solana', 'transactions') }}
  WHERE block_time >= '2021-01-01'
  {% if is_incremental() %}
    AND DATE_TRUNC('day', block_time) >= (SELECT MAX(block_date) FROM {{ this }})
  {% endif %}
)

SELECT
  cud.block_date,
  cud.block_time,
  cud.block_slot,
  cud.tx_id,
  cud.program_id,
  cud.instruction_count,
  cud.successful_instructions,
  cud.failed_instructions,
  tm.fee as transaction_fee,
  tm.tx_success,
  -- Calculate compute units based on instruction count and transaction success
  -- This is an approximation - actual compute units would need to be extracted from transaction metadata
  CASE 
    WHEN tm.tx_success THEN cud.instruction_count * 200000 -- Approximate compute units per instruction
    ELSE 0 
  END as estimated_compute_units_consumed,
  -- Add more granular compute unit tracking if available in your data
  CURRENT_TIMESTAMP as _inserted_timestamp
FROM compute_units_data cud
LEFT JOIN transaction_metadata tm ON cud.tx_id = tm.tx_id
WHERE cud.program_id IS NOT NULL
  AND cud.program_id != '' -- Filter out empty program IDs 