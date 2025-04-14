{{
  config(
    schema='solana_utils',
    alias='token_account_latest_state',
    materialized='table'
  )
}}

-- Get the relation object for the history model's target table
-- Use the alias defined in the history model's config
{% set history_model_relation = adapter.get_relation(
      database=target.database,
      schema=target.schema,
      identifier='alt_token_accounts_updates' 
) %}

-- Select the absolute latest state for each token account
-- from the output of the main token accounts updates model.

WITH source_updates AS (
    -- Reference the main model's *target table* directly using the relation object
    -- This breaks the DAG cycle but ensures reading from the correct table at runtime.
    -- IMPORTANT: Ensure this model runs AFTER alt_solana_utils_token_accounts_updates
    {% if history_model_relation %}
        SELECT * FROM {{ history_model_relation }}
    {% else %}
        -- Handle case where the relation doesn't exist yet (e.g., during parsing before first run)
        -- Manually define the structure to avoid using ref()
        SELECT 
            CAST(NULL AS VARCHAR) as token_account,
            CAST(NULL AS VARCHAR) as token_mint_address,
            CAST(NULL AS VARCHAR) as token_balance_owner,
            CAST(NULL AS VARCHAR) as event_type,
            CAST(NULL AS TIMESTAMP) as valid_from,
            CAST(NULL AS TIMESTAMP) as valid_to,
            CAST(NULL AS VARCHAR) as valid_from_instruction_uniq_id,
            CAST(NULL AS VARCHAR) as valid_to_instruction_uniq_id,
            CAST(NULL AS DATE) as valid_from_year,
            CAST(NULL AS DATE) as valid_to_year,
            CAST(NULL AS VARCHAR) as token_account_prefix
        WHERE 1=0 -- Ensure no rows are actually selected
    {% endif %}
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