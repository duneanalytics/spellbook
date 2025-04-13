{{
  config (
    schema='solana_utils',
    alias='token_account_raw_data',
    partition_by=['token_account_prefix', 'block_year'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='delete+insert'
  )
}}

{% set start_time = '2021-01-02' %}

--Init events contain mint address, and owner address
--Init v1
SELECT
  CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
  , call_block_time AS block_time
  , CAST(DATE_TRUNC('year', call_block_time) AS DATE) AS block_year
  , account_account AS token_account
  , SUBSTRING(account_account, 1, 2) AS token_account_prefix
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
FROM {{ source('spl_token_solana', 'spl_token_call_initializeaccount') }}
WHERE 1=1
  AND call_block_time >= TIMESTAMP '{{ start_time }}'
  {% if is_incremental() %}
  AND {{ incremental_predicate('call_block_time') }}
  {% endif %}

UNION ALL

--Init v2
SELECT
  CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
  , call_block_time AS block_time
  , CAST(DATE_TRUNC('year', call_block_time) AS DATE) AS block_year
  , account_account AS token_account
  , SUBSTRING(account_account, 1, 2) AS token_account_prefix
  , 'init' AS event_type
  , owner as account_owner
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
FROM {{ source('spl_token_solana', 'spl_token_call_initializeaccount2') }}
WHERE
  1=1
  AND call_block_time >= TIMESTAMP '{{ start_time }}'
  {% if is_incremental() %}
  AND {{ incremental_predicate('call_block_time') }}
  {% endif %}

UNION ALL

--Init v3
SELECT
  CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
  , call_block_time AS block_time
  , CAST(DATE_TRUNC('year', call_block_time) AS DATE) AS block_year
  , account_account AS token_account
  , SUBSTRING(account_account, 1, 2) AS token_account_prefix
  , 'init' AS event_type
  , owner as account_owner
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
FROM {{ source('spl_token_solana', 'spl_token_call_initializeaccount3') }}
WHERE 1=1
  AND call_block_time >= TIMESTAMP '{{ start_time }}'
  {% if is_incremental() %}
  AND {{ incremental_predicate('call_block_time') }}
  {% endif %}

UNION ALL


-- Owner Changes: Only owner changes, mint persists, mint address not in data
SELECT
  CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
  , call_block_time AS block_time
  , CAST(DATE_TRUNC('year', call_block_time) AS DATE) AS block_year
  , account_owned AS token_account
  , SUBSTRING(account_owned, 1, 2) AS token_account_prefix
  , 'owner_change' AS event_type
  , newAuthority as account_owner
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
FROM {{ source('spl_token_solana', 'spl_token_call_setauthority') }}
WHERE
  json_extract_scalar(authorityType, '$.AuthorityType') = 'AccountOwner'
  AND 1=1
  AND call_block_time >= TIMESTAMP '{{ start_time }}'
  {% if is_incremental() %}
  AND {{ incremental_predicate('call_block_time') }}
  {% endif %}

UNION ALL

--Closure events only contain the token account address
SELECT
  CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
  , call_block_time AS block_time
  , CAST(DATE_TRUNC('year', call_block_time) AS DATE) AS block_year
  , account_account AS token_account
  , SUBSTRING(account_account, 1, 2) AS token_account_prefix
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
FROM {{ source('spl_token_solana', 'spl_token_call_closeaccount') }}
WHERE 1=1
  AND call_block_time >= TIMESTAMP '{{ start_time }}'
  {% if is_incremental() %}
  AND {{ incremental_predicate('call_block_time') }}
  {% endif %}