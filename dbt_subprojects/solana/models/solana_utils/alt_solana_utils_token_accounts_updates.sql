{{
  config(
    schema='solana_utils',
    alias='alt_token_accounts_updates',
    materialized='incremental',
    incremental_strategy='merge',
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
      -- Select full history by joining raw data with a subquery identifying affected accounts
      (
          SELECT raw.*
          FROM {{ ref('solana_utils_token_account_raw_data') }} raw
          INNER JOIN (
              SELECT DISTINCT src.token_account
              FROM {{ ref('solana_utils_token_account_raw_data') }} src
              CROSS JOIN ( SELECT
                              -- Reinstate COALESCE for robustness against empty target table
                              COALESCE(MAX(valid_from_instruction_uniq_id), '0-0-0-0') AS max_id,
                              COALESCE(MAX(valid_from_year), DATE '1970-01-01') AS max_year
                           FROM {{ this }}
                         ) max_info
              WHERE
                src.block_year >= max_info.max_year
                AND src.instruction_uniq_id > max_info.max_id
          ) AS affected_accounts ON raw.token_account = affected_accounts.token_account
      ) AS incremental_source_data
    {% else %}
      {{ ref('solana_utils_token_account_raw_data') }} 
    {% endif %}
)

, final_selection AS (
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
  date_trunc('year', block_time) as valid_from_year,

  -- Use the next event's time/ID as the end of the valid interval, defaulting to infinity
  COALESCE(next_block_time, TIMESTAMP '9999-12-31 23:59:59') AS valid_to,
  COALESCE(next_instruction_uniq_id, '999999999-999999-9999-9999') AS valid_to_instruction_uniq_id,

  -- Calculate the year partition based on the valid_to date
  CAST(COALESCE(date_trunc('year', next_block_time), TIMESTAMP '9999-12-31') as date) as valid_to_year, -- Adjusted coalesce for DATE type
  token_account_prefix
FROM state_calculation
)

Select token_account, 
       token_mint_address,
       token_balance_owner,
       event_type,
       valid_from,
       valid_to,
       valid_from_instruction_uniq_id,
       valid_to_instruction_uniq_id,
       valid_from_year,
       valid_to_year,
       token_account_prefix
from final_selection
where account_owner is not null
and account_mint is not null