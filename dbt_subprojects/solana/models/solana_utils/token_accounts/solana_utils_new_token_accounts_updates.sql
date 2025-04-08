{{
  config(
    tags=['prod_exclude'],
    schema='solana_utils',
    alias='new_token_accounts_updates',
    partition_by=['token_account_prefix'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['token_account_prefix', 'token_account', 'valid_from', 'block_slot', 'tx_index', 'inner_instruction_index', 'outer_instruction_index', 'event_type'],
  )
}}

WITH raw_events AS (
  SELECT
    block_time
    , block_slot
    , tx_index
    , inner_instruction_index
    , outer_instruction_index
    , token_account
    , token_account_prefix
    , event_type
    , account_owner
    , account_mint
  FROM {{ ref('solana_utils_token_account_raw_data') }}
  {% if is_incremental() %}
  WHERE {{ incremental_predicate('block_time') }}
  {% else %}
  WHERE block_time >= TIMESTAMP '2025-01-01'
  {% endif %}
)
{% if is_incremental() %}
, latest_existing AS (
  -- Get most recent active row per token_account from the target table
  -- needed for window functions downstream, in case source incremental filter removes token_account
  SELECT
    valid_from AS block_time
    , block_slot
    , tx_index
    , inner_instruction_index
    , outer_instruction_index
    , token_account
    , token_account_prefix
    , event_type
    , token_balance_owner AS account_owner
    , token_mint_address AS account_mint
  FROM {{ this }}
  WHERE 
    valid_to = TIMESTAMP '9999-12-31 23:59:59' --only include rows that are still active
    AND token_account IN (SELECT DISTINCT token_account FROM raw_events) --only include rows that are in the raw_events
)
{% endif %}
, combined AS (
  -- Union new raw events with the last known state for each token_account
  SELECT *, 'source' as source_type FROM raw_events
  {% if is_incremental() %}
  UNION ALL
  SELECT *, 'target' as source_type FROM latest_existing
  {% endif %}
)
, deduplicated AS (
  -- since account_mint is forward filled in target table, we need to deduplicate the events if exist in both source and target after union
  SELECT 
    token_account
    , token_account_prefix
    , event_type
    , account_owner
    , account_mint
    , block_time
    , block_slot
    , tx_index
    , inner_instruction_index
    , outer_instruction_index
  FROM (
    SELECT *
      , ROW_NUMBER() OVER (
        PARTITION BY token_account, token_account_prefix, event_type, block_slot, tx_index, inner_instruction_index, outer_instruction_index
        ORDER BY 
          CASE WHEN source_type = 'source' THEN 1 ELSE 2 END, -- prefer source rows
          CASE WHEN account_mint IS NULL THEN 1 ELSE 2 END -- prefer null account_mint from source
      ) as rn
    FROM combined
  ) ranked
  WHERE rn = 1
)
, windowed AS (
  -- Apply window functions
  SELECT
    token_account
    , token_account_prefix
    , event_type
    , account_owner
    -- Forward-fill mint across all events
    , MAX(CASE WHEN account_mint IS NOT NULL THEN account_mint END)
      OVER (
        PARTITION BY token_account
        ORDER BY block_slot, tx_index, inner_instruction_index, outer_instruction_index ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS last_non_null_account_mint
    , block_time AS valid_from
    , LEAD(block_time) OVER (PARTITION BY token_account ORDER BY block_slot, tx_index, inner_instruction_index, outer_instruction_index ASC) AS raw_valid_to
    , block_slot
    , tx_index
    , inner_instruction_index
    , outer_instruction_index
  FROM deduplicated
)
, final AS (
  -- Final output with proper mint logic and closed intervals
  SELECT
    token_account
    , token_account_prefix
    , event_type
    , account_owner AS token_balance_owner
    , last_non_null_account_mint AS token_mint_address -- owner_changed & closed will forward fill the mint address from init
    , valid_from
    , COALESCE(raw_valid_to, TIMESTAMP '9999-12-31 23:59:59') AS valid_to
    , block_slot
    , tx_index
    , inner_instruction_index
    , outer_instruction_index
  FROM windowed
)

SELECT *
FROM final