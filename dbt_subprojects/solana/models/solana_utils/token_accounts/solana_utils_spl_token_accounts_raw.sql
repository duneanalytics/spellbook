{{
  config (
    schema='solana_utils'
    , alias='spl_token_accounts_raw'
    , materialized='table'
    , partition_by=['address_prefix']
    , file_format='delta'
    , unique_key=['address', 'address_prefix', 'unique_instruction_key']
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
  , account_account AS address
  , SUBSTRING(account_account, 1, 3) AS address_prefix
  , 'init' AS event_type
  , account_owner AS token_balance_owner
  , account_mint AS token_mint_address
  -- constructing an artificial instruction_uniq_id to order instructions using one string column
  -- lpads are chosen carefully to be lexicographically sortable
  -- we can sort the instructions by this column to get the correct order
  , CONCAT(
      lpad(cast(call_block_slot as varchar), 12, '0'), '-', 
      lpad(cast(call_tx_index as varchar), 6, '0'), '-', 
      lpad(cast(coalesce(call_outer_instruction_index, 0) as varchar), 4, '0'), '-', 
      lpad(cast(coalesce(call_inner_instruction_index, 0) as varchar), 4, '0')
    ) AS unique_instruction_key
FROM {{ source('spl_token_solana', 'spl_token_call_initializeaccount') }}
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
  , account_account AS address
  , SUBSTRING(account_account, 1, 3) AS address_prefix
  , 'init' AS event_type
  , owner as token_balance_owner
  , account_mint AS token_mint_address
  -- constructing an artificial instruction_uniq_id to order instructions using one string column
  -- lpads are chosen carefully to be lexicographically sortable
  -- we can sort the instructions by this column to get the correct order
  , CONCAT(
      lpad(cast(call_block_slot as varchar), 12, '0'), '-', 
      lpad(cast(call_tx_index as varchar), 6, '0'), '-', 
      lpad(cast(coalesce(call_outer_instruction_index, 0) as varchar), 4, '0'), '-', 
      lpad(cast(coalesce(call_inner_instruction_index, 0) as varchar), 4, '0')
    ) AS unique_instruction_key
FROM {{ source('spl_token_solana', 'spl_token_call_initializeaccount2') }}
WHERE
  1=1
  
  {% if is_incremental() %}
  AND {{ incremental_predicate('call_block_time') }}
  {% endif %}

UNION ALL

--Init v3
SELECT
  CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
  , call_block_time AS block_time
  , CAST(DATE_TRUNC('year', call_block_time) AS DATE) AS block_year
  , account_account AS address
  , SUBSTRING(account_account, 1, 3) AS address_prefix
  , 'init' AS event_type
  , owner as token_balance_owner
  , account_mint AS token_mint_address
  -- constructing an artificial instruction_uniq_id to order instructions using one string column
  -- lpads are chosen carefully to be lexicographically sortable
  -- we can sort the instructions by this column to get the correct order
  , CONCAT(
      lpad(cast(call_block_slot as varchar), 12, '0'), '-', 
      lpad(cast(call_tx_index as varchar), 6, '0'), '-', 
      lpad(cast(coalesce(call_outer_instruction_index, 0) as varchar), 4, '0'), '-', 
      lpad(cast(coalesce(call_inner_instruction_index, 0) as varchar), 4, '0')
    ) AS unique_instruction_key
FROM {{ source('spl_token_solana', 'spl_token_call_initializeaccount3') }}
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
  , account_owned AS address
  , SUBSTRING(account_owned, 1, 3) AS address_prefix
  , 'owner_change' AS event_type
  , newAuthority as token_balance_owner
  , null as token_mint_address
  -- constructing an artificial instruction_uniq_id to order instructions using one string column
  -- lpads are chosen carefully to be lexicographically sortable
  -- we can sort the instructions by this column to get the correct order
  , CONCAT(
      lpad(cast(call_block_slot as varchar), 12, '0'), '-', 
      lpad(cast(call_tx_index as varchar), 6, '0'), '-', 
      lpad(cast(coalesce(call_outer_instruction_index, 0) as varchar), 4, '0'), '-', 
      lpad(cast(coalesce(call_inner_instruction_index, 0) as varchar), 4, '0')
    ) AS unique_instruction_key
FROM {{ source('spl_token_solana', 'spl_token_call_setauthority') }}
WHERE
  json_extract_scalar(authorityType, '$.AuthorityType') = 'AccountOwner'
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
  , account_account AS address
  , SUBSTRING(account_account, 1, 3) AS address_prefix
  , 'close' AS event_type
  , null as token_balance_owner
  , null as token_mint_address
  -- constructing an artificial instruction_uniq_id to order instructions using one string column
  -- lpads are chosen carefully to be lexicographically sortable
  -- we can sort the instructions by this column to get the correct order
  , CONCAT(
      lpad(cast(call_block_slot as varchar), 12, '0'), '-', 
      lpad(cast(call_tx_index as varchar), 6, '0'), '-', 
      lpad(cast(coalesce(call_outer_instruction_index, 0) as varchar), 4, '0'), '-', 
      lpad(cast(coalesce(call_inner_instruction_index, 0) as varchar), 4, '0')
    ) AS unique_instruction_key
FROM {{ source('spl_token_solana', 'spl_token_call_closeaccount') }}
WHERE 1=1
  {% if is_incremental() %}
  AND {{ incremental_predicate('call_block_time') }}
  {% endif %}