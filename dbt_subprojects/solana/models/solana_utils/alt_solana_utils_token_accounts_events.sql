{{
  config(
    schema='solana_utils',
    alias='token_accounts_events',
    materialized='incremental',
    file_format='delta',
    partition_by=['token_account_prefix', 'event_time_month'],
    incremental_strategy='merge',
    incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.event_time')],
    unique_key=['token_account','instruction_uniq_id']
  )
}}

{% set start_date = '2025-04-01' %}

WITH 
-- Add monthly partitioning for better join performance
initializations_with_month AS (
  SELECT
    token_account,
    account_owner,
    account_mint,
    event_time,
    'init' AS event_type,
    token_account_prefix,
    block_month,
    instruction_uniq_id
  FROM {{ ref('alt_solana_utils_token_account_initializations') }}
),

-- Pre-filter authority changes before joining
filtered_authority_changes AS (
  SELECT
    account_owned,
    newAuthority,
    call_block_time,
    SUBSTRING(account_owned, 1, 1) AS token_account_prefix,
    DATE_TRUNC('month', call_block_time) AS call_block_month,
    CONCAT(
      CAST(call_block_slot AS VARCHAR), '-', 
      CAST(call_tx_index AS VARCHAR), '-', 
      CAST(COALESCE(call_outer_instruction_index,0) AS VARCHAR), '-', 
      CAST(COALESCE(call_inner_instruction_index, 0) AS VARCHAR)
    ) AS instruction_uniq_id
  FROM {{ source('spl_token_solana', 'spl_token_call_setauthority') }}
  WHERE json_extract_scalar(authorityType, '$.AuthorityType') = 'AccountOwner'
    AND call_block_time >= timestamp '{{start_date}}'
    {% if is_incremental() %}
    AND {{ incremental_predicate('call_block_time') }}
    {% endif %}
),

-- Pre-filter account closures before joining
filtered_account_closures AS (
  SELECT
    account_account,
    call_block_time,
    SUBSTRING(account_account, 1, 1) AS token_account_prefix,
    DATE_TRUNC('month', call_block_time) AS call_block_month,
    CONCAT(
      CAST(call_block_slot AS VARCHAR), '-', 
      CAST(call_tx_index AS VARCHAR), '-', 
      CAST(COALESCE(call_outer_instruction_index,0) AS VARCHAR), '-', 
      CAST(COALESCE(call_inner_instruction_index, 0) AS VARCHAR)
    ) AS instruction_uniq_id
  FROM {{ source('spl_token_solana', 'spl_token_call_closeaccount') }}
  WHERE call_block_time >= timestamp '{{start_date}}'
    {% if is_incremental() %}
    AND {{ incremental_predicate('call_block_time') }}
    {% endif %}
),

-- Use window function instead of GROUP BY to find latest initialization
latest_init_before_transfer AS (
  SELECT
    t.token_account,
    t.account_mint,
    t.transfer_time,
    t.init_time AS latest_init_time,
    t.instruction_uniq_id AS init_instruction_uniq_id
  FROM (
    SELECT
      i.token_account,
      i.account_mint,
      sa.call_block_time AS transfer_time,
      i.event_time AS init_time,
      i.instruction_uniq_id,
      ROW_NUMBER() OVER (
        PARTITION BY i.token_account, sa.call_block_time
        ORDER BY i.event_time DESC, i.instruction_uniq_id DESC
      ) AS rn
    FROM filtered_authority_changes sa
    JOIN initializations_with_month i
      ON sa.account_owned = i.token_account
      AND i.event_time < sa.call_block_time
      -- for better performance
      AND sa.token_account_prefix = i.token_account_prefix
      AND i.block_month <= sa.call_block_month
  ) t
  WHERE t.rn = 1
),

latest_init_before_closure AS (
  SELECT
    t.token_account,
    t.account_mint,
    t.closure_time,
    t.init_time AS latest_init_time,
    t.instruction_uniq_id AS init_instruction_uniq_id
  FROM (
    SELECT
      i.token_account,
      i.account_mint,
      c.call_block_time AS closure_time,
      i.event_time AS init_time,
      i.instruction_uniq_id,
      ROW_NUMBER() OVER (
        PARTITION BY i.token_account, c.call_block_time
        ORDER BY i.event_time DESC, i.instruction_uniq_id DESC
      ) AS rn
    FROM filtered_account_closures c
    JOIN initializations_with_month i
      ON c.account_account = i.token_account
      AND i.event_time < c.call_block_time
      -- for better performance
      AND c.token_account_prefix = i.token_account_prefix
      AND i.block_month <= c.call_block_month
  ) t
  WHERE t.rn = 1
),

ownership_transfers AS (
  SELECT
    sa.account_owned AS token_account,
    sa.newAuthority AS account_owner,
    li.account_mint,
    sa.call_block_time AS event_time,
    'owner_change' AS event_type,
    sa.token_account_prefix, -- Use the pre-calculated prefix
    sa.call_block_month AS event_time_month,
    sa.instruction_uniq_id
  FROM filtered_authority_changes sa
  LEFT JOIN latest_init_before_transfer li 
    ON sa.account_owned = li.token_account
    AND sa.call_block_time = li.transfer_time
),

closures AS (
  SELECT
    c.account_account AS token_account,
    NULL AS account_owner,
    li.account_mint,
    c.call_block_time AS event_time,
    'close' AS event_type,
    c.token_account_prefix, -- Use the pre-calculated prefix
    c.call_block_month AS event_time_month,
    c.instruction_uniq_id
  FROM filtered_account_closures c
  LEFT JOIN latest_init_before_closure li 
    ON c.account_account = li.token_account
    AND c.call_block_time = li.closure_time
)

SELECT 
  token_account,
  account_owner,
  account_mint,
  event_time,
  event_type,
  token_account_prefix,
  block_month AS event_time_month,
  instruction_uniq_id
FROM initializations_with_month
{% if is_incremental() %}
WHERE {{ incremental_predicate('event_time') }}
{% endif %}

UNION ALL
SELECT
  token_account,
  account_owner,
  account_mint,
  event_time,
  event_type,
  token_account_prefix,
  event_time_month,
  instruction_uniq_id
FROM ownership_transfers
UNION ALL
SELECT
  token_account,
  account_owner,
  account_mint,
  event_time,
  event_type,
  token_account_prefix,
  event_time_month,
  instruction_uniq_id
FROM closures 