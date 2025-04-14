{{
  config(
    schema='solana_utils',
    alias='alt_token_accounts_updates',
    materialized='incremental',
    incremental_strategy='merge',
    partition_by=['token_account_prefix', 'valid_from_year'],
    unique_key=['token_account', 'valid_from_instruction_uniq_id']
  )
}}

-- Determine the high-water mark for incremental processing based on the *target* table (this model)
{% set last_processed_id = '0' %} -- Default for first run
{% set start_year_filter_date = modules.datetime.date(1970, 1, 1) %} -- Default date object

{% if is_incremental() %}
    {% set this_relation_exists = load_relation(this) is not none %}
    {% if this_relation_exists %}
        {% set max_values_query %}
            SELECT
                COALESCE(MAX(valid_from_instruction_uniq_id), '0') AS max_id,
                COALESCE(MAX(valid_from_year), DATE '1970-01-01') AS max_year -- Use MAX on the stored valid_from_year
            FROM {{ this }}
        {% endset %}
        {% set max_values_result = run_query(max_values_query) %}
        {% if execute and max_values_result.rows %}
            {% set last_processed_id = max_values_result.rows[0]['max_id'] %}
            {% set start_year_filter_date = max_values_result.rows[0]['max_year'] %}
        {% endif %}
    {% endif %}
{% endif %}

WITH 

raw_data_source AS (
    SELECT 
        token_account,
        account_owner,
        account_mint,
        instruction_uniq_id,
        block_time,
        event_type,
        token_account_prefix,
        date_trunc('year', block_time) as block_year -- Ensure block_year is available/calculated
    FROM {{ ref('solana_utils_token_account_raw_data') }} -- Assuming this ref is correct
    
    {% if is_incremental() %}
    -- Filter for new raw events since the last run
    WHERE instruction_uniq_id > '{{ last_processed_id }}' 
      -- Optimization: Only scan partitions potentially containing new data
      AND date_trunc('year', block_time) >= DATE '{{ start_year_filter_date.strftime("%Y-%m-%d") }}' 
    {% endif %}
),

-- Identify accounts with new activity
affected_accounts AS (
    SELECT DISTINCT token_account
    FROM raw_data_source
),

-- Get the latest known state for affected accounts from the state table
previous_latest_states AS (
    SELECT
        ls.token_account,
        ls.token_balance_owner AS account_owner,
        ls.token_mint_address AS account_mint,
        ls.last_processed_instruction_id AS instruction_uniq_id,
        ls.last_processed_block_time AS block_time,
        'state_record' AS event_type, -- Mark these records
        NULL AS token_account_prefix, -- Align columns with new_raw_events
        NULL AS block_year -- Align columns
    FROM {{ ref('token_account_latest_state') }} ls
    INNER JOIN affected_accounts aa ON ls.token_account = aa.token_account

    -- We only need the state if it's *before or exactly at* the start of our new batch
    WHERE ls.last_processed_instruction_id <= '{{ last_processed_id }}'
),

-- Select the relevant new raw events for affected accounts
new_raw_events AS (
    SELECT 
        raw.token_account,
        raw.account_owner,
        raw.account_mint,
        raw.instruction_uniq_id,
        raw.block_time,
        raw.event_type,
        raw.token_account_prefix,
        raw.block_year
    FROM raw_data_source raw
    INNER JOIN affected_accounts aa ON raw.token_account = aa.token_account
    -- The incremental filter is already applied in raw_data_source
),

-- Combine previous state with new events
combined_data AS (
    SELECT * FROM previous_latest_states
    UNION ALL
    SELECT * FROM new_raw_events
),

-- Calculate state intervals using window functions on the combined data
state_calculation AS (
  SELECT
    token_account,
    account_owner,
    account_mint, -- Original mint needed for CASE statement later
    instruction_uniq_id,
    block_time,
    event_type,
    token_account_prefix,
    -- block_year, -- Not strictly needed after this point unless used in final output

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

  FROM combined_data -- Changed source here
)

, final_selection AS (
SELECT
  token_account,
  account_owner as token_balance_owner,
  CASE
    WHEN event_type = 'owner_change' THEN last_non_null_account_mint
    ELSE account_mint
  END AS token_mint_address,
  event_type,

  block_time AS valid_from,
  instruction_uniq_id as valid_from_instruction_uniq_id,
  date_trunc('year', block_time) as valid_from_year,

  COALESCE(next_block_time, TIMESTAMP '9999-12-31 23:59:59') AS valid_to,
  COALESCE(next_instruction_uniq_id, '999999999-999999-9999-9999') AS valid_to_instruction_uniq_id,
  CAST(COALESCE(date_trunc('year', next_block_time), TIMESTAMP '9999-12-31') as date) as valid_to_year, 
  token_account_prefix
FROM state_calculation
WHERE event_type != 'state_record' -- Exclude the placeholder state records
)

-- Final selection with filters and incremental logic applied by dbt merge
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
where token_balance_owner is not null
and token_mint_address is not null