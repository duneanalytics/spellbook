{{
  config(
    schema='solana_utils',
    alias='alt_token_accounts_updates',
    materialized='incremental',
    partition_by=['token_account_prefix', 'valid_to_year'],
    unique_key=['token_account', 'valid_to_year', 'valid_from_instruction_uniq_id']
  )
}}

-- Common Table Expression for calculating state intervals using window functions
WITH state_calculation AS (
  SELECT
    token_account,
    account_owner,
    account_mint, -- Original mint needed for CASE statement later
    instruction_uniq_id,
    block_time,
    event_type,
    token_account_prefix,

    -- get the latest non-null account_mint up to this point
    MAX(CASE WHEN account_mint IS NOT NULL THEN account_mint END)
      OVER (
        PARTITION BY token_account
        ORDER BY instruction_uniq_id ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS last_non_null_account_mint,

    -- Capture next event's time and ID using LEAD
    LEAD(block_time, 1) OVER (PARTITION BY token_account ORDER BY instruction_uniq_id ASC) AS next_block_time,
    LEAD(instruction_uniq_id, 1) OVER (PARTITION BY token_account ORDER BY instruction_uniq_id ASC) AS next_instruction_uniq_id

  FROM
    {% if is_incremental() %}
    -- For incremental runs, process only the full history for those accounts that have had a change within YTD
    -- Since the upstream model is partitioned by year, this will skip 
    (
      SELECT raw.*
      FROM {{ ref('solana_utils_token_account_raw_data') }} raw
      INNER JOIN (
        -- Subquery to find accounts with events newer than the max processed ID in the target table
        SELECT DISTINCT token_account, block_year
        FROM {{ ref('solana_utils_token_account_raw_data') }}
        WHERE block_year >= (SELECT MAX(valid_to_year) FROM {{ this }})
      ) affected_accounts ON raw.token_account = affected_accounts.token_account and raw.block_year = affected_accounts.block_year
    ) AS incremental_source_data
    {% else %}
    -- In a full refresh, process all data from the source table
    {{ ref('solana_utils_token_account_raw_data') }} AS full_refresh_source_data
    {% endif %}
)

-- Final selection and transformation logic
SELECT
  token_account,
  account_owner as token_balance_owner,
  CASE
    -- Use the carried-forward mint if the original was null due to owner change event logic (if applicable)
    WHEN event_type = 'owner_change' THEN last_non_null_account_mint
    ELSE account_mint
  END AS token_mint_address,
  event_type,

  -- Use current event's time/ID as the start of the valid interval
  block_time AS valid_from,
  
  instruction_uniq_id as valid_from_instruction_uniq_id,

  -- Use the next event's time/ID as the end of the valid interval, defaulting to infinity
  COALESCE(next_block_time, TIMESTAMP '9999-12-31 23:59:59') AS valid_to,
  COALESCE(next_instruction_uniq_id, '999999999-999999-9999-9999') AS valid_to_instruction_uniq_id,

  -- Calculate the year partition based on the valid_to date
  CAST(COALESCE(date_trunc('year', next_block_time), TIMESTAMP '9999-12-31') as date) as valid_to_year, -- Adjusted coalesce for DATE type
  token_account_prefix

FROM state_calculation

-- Apply final filters (ensure these columns are available after window functions)
WHERE account_owner IS NOT NULL
  AND account_mint IS NOT NULL -- Filter based on original mint, adjust if needed based on the CASE logic intent

-- No additional incremental filter needed here, as the merge strategy handles updates/inserts based on the unique_key
