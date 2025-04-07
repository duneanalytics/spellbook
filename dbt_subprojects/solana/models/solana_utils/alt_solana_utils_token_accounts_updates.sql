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
    -- Calculate account_mint using LAST_VALUE
    -- When an account is initialized, the mint is carried forward
    -- When an account owner changes, the mint is carried forward
    -- When an account closes, the mint resets to NULL
    CASE
      WHEN event_type = 'owner_change' THEN
        LAST_VALUE(account_mint) OVER (
          PARTITION BY token_account
          ORDER BY instruction_uniq_id ASC 
        )
      ELSE account_mint
    END AS account_mint,
    event_time AS valid_from,
    instruction_uniq_id as valid_from_instruction_uniq_id,
    -- the next event time is the valid_to
    LEAD(event_time, 1) OVER (PARTITION BY token_account ORDER BY event_time ASC) AS valid_to,
    LEAD(instruction_uniq_id, 1) OVER (PARTITION BY token_account ORDER BY instruction_uniq_id ASC) AS valid_to_instruction_uniq_id,
    block_month,
    event_type,
    token_account_prefix
  FROM {{ ref('alt_solana_utils_token_account_raw_data') }}
)

-- Final selection from the combined CTE
SELECT
  token_account,
  account_owner as token_balance_owner,
  account_mint as token_mint_address,
  event_type,
  valid_from,
  -- constructing a valid_to in the future to avoid nulls to handle joins
  COALESCE(valid_to, TIMESTAMP '9999-12-31 23:59:59') AS valid_to, 
  valid_from_instruction_uniq_id,
  -- constructing a valid_to_instruction_uniq_id in the future to avoid nulls to handle joins
  COALESCE(valid_to_instruction_uniq_id, '999999999-999999-9999-9999') AS valid_to_instruction_uniq_id,
  token_account_prefix
FROM combined_events