{{
  config(
    schema='solana_utils',
    tags=['prod_exclude'],
    alias='alt_token_2022_account_raw_data',
    partition_by=['token_account_prefix', 'block_year'],
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['token_account_prefix', 'token_account', 'instruction_uniq_id']
  )
}}

-- This model gathers all the events that can happen to a token account, which change the state of the token account
-- state includes: owner_address and token_program, which can both change over time
-- we are repartitioning the data by token_account_prefix and block_year
-- This really helps the downstream calculations that all use window functions partitioned by token_account

--Init events contain mint address, and owner address
--Init v1
SELECT
  CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
  , call_block_time AS block_time
  , CAST(DATE_TRUNC('year', call_block_time) AS DATE) AS block_year
  , account_accountToInitialize AS token_account
  , SUBSTRING(account_accountToInitialize, 1, 2) AS token_account_prefix
  , 'init' AS event_type
  , account_owner
  , account_mint
  -- constructing an artificial instruction_uniq_id to order instructions using one string column
  -- lpads are chosen carefully to be lexicographically sortable
  -- we can sort the instructions by this column to get the correct order
  , CONCAT(
      lpad(cast(call_block_slot as varchar), 12, '0'), '-',
      lpad(cast(call_tx_index as varchar), 6, '0'), '-',
      lpad(cast(coalesce(call_outer_instruction_index, 0) as varchar), 4, '0'), '-',
      lpad(cast(coalesce(call_inner_instruction_index, 0) as varchar), 4, '0')
    ) AS instruction_uniq_id
FROM {{ source('spl_token_2022_solana', 'spl_token_2022_call_initializeaccount') }}
WHERE 1=1
{% if is_incremental() %}
  AND {{ incremental_predicate('call_block_time') }}
{% endif %}

UNION ALL

--Init v2
SELECT
  CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
  , call_block_time AS block_time
  , CAST(DATE_TRUNC('year', call_block_time) AS DATE) AS block_year
  , account_initializeAccount AS token_account
  , SUBSTRING(account_initializeAccount, 1, 2) AS token_account_prefix
  , 'init' AS event_type
  , owner as account_owner
  , account_associatedMint AS account_mint
  -- constructing an artificial instruction_uniq_id to order instructions using one string column
  -- lpads are chosen carefully to be lexicographically sortable
  -- we can sort the instructions by this column to get the correct order
  , CONCAT(
      lpad(cast(call_block_slot as varchar), 12, '0'), '-',
      lpad(cast(call_tx_index as varchar), 6, '0'), '-',
      lpad(cast(coalesce(call_outer_instruction_index, 0) as varchar), 4, '0'), '-',
      lpad(cast(coalesce(call_inner_instruction_index, 0) as varchar), 4, '0')
    ) AS instruction_uniq_id
FROM {{ source('spl_token_2022_solana', 'spl_token_2022_call_initializeaccount2') }}
WHERE 1=1
{% if is_incremental() %}
  AND {{ incremental_predicate('call_block_time') }}
{% endif %}

UNION ALL

--Init v3
SELECT
  CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
  , call_block_time AS block_time
  , CAST(DATE_TRUNC('year', call_block_time) AS DATE) AS block_year
  , account_initializeAccount AS token_account
  , SUBSTRING(account_initializeAccount, 1, 2) AS token_account_prefix
  , 'init' AS event_type
  , owner as account_owner
  , account_associatedMint AS account_mint
  -- constructing an artificial instruction_uniq_id to order instructions using one string column
  -- lpads are chosen carefully to be lexicographically sortable
  -- we can sort the instructions by this column to get the correct order
  , CONCAT(
      lpad(cast(call_block_slot as varchar), 12, '0'), '-',
      lpad(cast(call_tx_index as varchar), 6, '0'), '-',
      lpad(cast(coalesce(call_outer_instruction_index, 0) as varchar), 4, '0'), '-',
      lpad(cast(coalesce(call_inner_instruction_index, 0) as varchar), 4, '0')
    ) AS instruction_uniq_id
FROM {{ source('spl_token_2022_solana', 'spl_token_2022_call_initializeaccount3') }}
WHERE 1=1
{% if is_incremental() %}
  AND {{ incremental_predicate('call_block_time') }}
{% endif %}

UNION ALL


-- Owner Changes: Only owner changes, mint persists, mint address not in data
SELECT
  CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
  , call_block_time AS block_time
  , CAST(DATE_TRUNC('year', call_block_time) AS DATE) AS block_year
  -- this is actually the token account address, the decoding pipeline is wrong here
  , account_mint AS token_account
  , SUBSTRING(account_mint, 1, 2) AS token_account_prefix
  , 'owner_change' AS event_type
  , newAuthority AS account_owner
  , null as account_mint
     -- constructing an artificial instruction_uniq_id to order instructions using one string column
     -- lpads are chosen carefully to be lexicographically sortable
     -- we can sort the instructions by this column to get the correct order
  , CONCAT(
      lpad(cast(call_block_slot as varchar), 12, '0'), '-',
      lpad(cast(call_tx_index as varchar), 6, '0'), '-',
      lpad(cast(coalesce(call_outer_instruction_index, 0) as varchar), 4, '0'), '-',
      lpad(cast(coalesce(call_inner_instruction_index, 0) as varchar), 4, '0')
    ) AS instruction_uniq_id
  FROM {{ source('spl_token_2022_solana', 'spl_token_2022_call_setauthority') }}
  WHERE json_extract_scalar(authorityType, '$.AuthorityType') = 'AccountOwner'
  AND 1=1
  {% if is_incremental() %}
    AND {{ incremental_predicate('call_block_time') }}
  {% endif %}

UNION ALL

--Closure events only contain the token account address
SELECT
  CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
  , call_block_time AS block_time
  , CAST(DATE_TRUNC('year', call_block_time) AS DATE) AS block_year
  , account_closeAccount AS token_account
  , SUBSTRING(account_closeAccount, 1, 2) AS token_account_prefix
  , 'close' AS event_type
  , null as account_owner
  , null as account_mint
  -- constructing an artificial instruction_uniq_id to order instructions using one string column
  -- lpads are chosen carefully to be lexicographically sortable
  -- we can sort the instructions by this column to get the correct order
  , CONCAT(
      lpad(cast(call_block_slot as varchar), 12, '0'), '-',
      lpad(cast(call_tx_index as varchar), 6, '0'), '-',
      lpad(cast(coalesce(call_outer_instruction_index, 0) as varchar), 4, '0'), '-',
      lpad(cast(coalesce(call_inner_instruction_index, 0) as varchar), 4, '0')
    ) AS instruction_uniq_id
  FROM {{ source('spl_token_2022_solana', 'spl_token_2022_call_closeaccount') }}
  WHERE 1=1
    {% if is_incremental() %}
    AND {{ incremental_predicate('call_block_time') }}
    {% endif %} 