{{
  config(
    schema='solana_utils',
    alias='token_account_latest_state',
    materialized='table',
    partition_by='token_account_prefix'
  )
}}

WITH source_updates AS (
    -- Reference the main model that calculates historical states
    -- IMPORTANT: Ensure this model runs AFTER alt_solana_utils_token_accounts_updates
    SELECT * FROM {{ ref('alt_solana_utils_token_accounts_updates') }}
),

ranked_states AS (
    SELECT
        token_account,
        token_balance_owner, -- The owner associated with the latest state
        token_mint_address,  -- The mint associated with the latest state
        token_account_prefix,
        -- Choose the instruction ID that represents this state's validity start
        valid_from_instruction_uniq_id AS last_processed_instruction_id, 
        
        -- Keep the valid_from time for potential future use or debugging
        valid_from AS last_processed_block_time, 

        -- Rank states within each account, latest first
        ROW_NUMBER() OVER (
            PARTITION BY token_account 
            ORDER BY valid_from_instruction_uniq_id DESC
        ) as rn
    FROM source_updates
    -- The main model already applies `token_balance_owner is not null and token_mint_address is not null`
)

SELECT
    token_account,
    token_account_prefix,
    token_balance_owner,
    token_mint_address,
    last_processed_instruction_id,
    last_processed_block_time
FROM ranked_states
WHERE rn = 1 