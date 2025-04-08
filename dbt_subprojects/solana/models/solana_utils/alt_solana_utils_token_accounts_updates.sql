{{
  config(
    schema='solana_utils',
    alias='alt_token_accounts_updates',
    materialized='table',
    partition_by=['token_account_prefix']
  )
}}


-- Combine state calculation and interval determination in one CTE
WITH combined_events AS (
  SELECT
    token_account,
    account_owner,
    account_mint, -- Keep original mint for later logic
    
    -- get the latest non-null account_mint up to this point
    MAX(CASE WHEN account_mint IS NOT NULL THEN account_mint END)
      OVER (
        PARTITION BY token_account
        ORDER BY block_time, block_slot, tx_index, outer_instruction_index, inner_instruction_index ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS last_non_null_account_mint,

    -- Capture start time, slot, and indices
    block_time AS valid_from,
    block_slot as valid_from_block_slot,
    tx_index as valid_from_tx_index,
    outer_instruction_index as valid_from_outer_index,
    inner_instruction_index as valid_from_inner_index,

    -- Calculate end time, slot, and indices using granular LEAD
    LEAD(block_time, 1) OVER (PARTITION BY token_account ORDER BY block_time, block_slot, tx_index, outer_instruction_index, inner_instruction_index ASC) AS valid_to,
    LEAD(block_slot, 1) OVER (PARTITION BY token_account ORDER BY block_time, block_slot, tx_index, outer_instruction_index, inner_instruction_index ASC) AS valid_to_block_slot,
    LEAD(tx_index, 1) OVER (PARTITION BY token_account ORDER BY block_time, block_slot, tx_index, outer_instruction_index, inner_instruction_index ASC) AS valid_to_tx_index,
    LEAD(outer_instruction_index, 1) OVER (PARTITION BY token_account ORDER BY block_time, block_slot, tx_index, outer_instruction_index, inner_instruction_index ASC) AS valid_to_outer_index,
    LEAD(inner_instruction_index, 1) OVER (PARTITION BY token_account ORDER BY block_time, block_slot, tx_index, outer_instruction_index, inner_instruction_index ASC) AS valid_to_inner_index,
    
    block_month,
    event_type,
    token_account_prefix
  FROM {{ ref('solana_utils_token_account_raw_data') }}
)

-- Final selection from the combined CTE
SELECT
  token_account,
  account_owner as token_balance_owner,
  CASE
    WHEN event_type = 'owner_change' THEN last_non_null_account_mint
    ELSE account_mint
  END AS token_mint_address,
  event_type,
  
  -- Select start time, slot and indices
  valid_from,
  valid_from_block_slot,
  valid_from_tx_index,
  valid_from_outer_index,
  valid_from_inner_index,
  
  -- Constructing a valid_to in the future to avoid nulls to handle joins
  -- Note: The corresponding slot/indices will be null for the last event, which is expected.
  COALESCE(valid_to, TIMESTAMP '9999-12-31 23:59:59') AS valid_to,
  valid_to_block_slot,
  valid_to_tx_index, 
  valid_to_outer_index,
  valid_to_inner_index,
  token_account_prefix
FROM combined_events