{{
  config(
    schema='solana_utils',
    alias='token_2022_account_initializations',
    materialized='incremental',
    file_format='delta',
    partition_by=['token_account_prefix'],
    incremental_strategy='merge',
    incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.event_time')],
    unique_key=['token_account', 'instruction_uniq_id']
  )
}}

--Init events contain mint address, and owner address
--Init v1
SELECT
  account_accountToInitialize AS token_account,
  account_owner,
  account_mint,
  call_block_time AS event_time,
  DATE_TRUNC('month', call_block_time) AS block_month,
  SUBSTRING(account_accountToInitialize, 1, 2) AS token_account_prefix,
  'init' AS event_type,
  CONCAT(
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
  account_initializeAccount AS token_account,
  owner as account_owner,
  account_associatedMint AS account_mint,
  call_block_time AS event_time,
  DATE_TRUNC('month', call_block_time) AS block_month,
  SUBSTRING(account_initializeAccount, 1, 2) AS token_account_prefix,
  'init' AS event_type,
  CONCAT(
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
  account_initializeAccount AS token_account,
  owner as account_owner,
  account_associatedMint AS account_mint,
  call_block_time AS event_time,
  DATE_TRUNC('month', call_block_time) AS block_month,
  SUBSTRING(account_initializeAccount, 1, 2) AS token_account_prefix,
  'init' AS event_type,
  CONCAT(
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
  account_mint AS token_account, -- this is actually the token account address, the decoding pipeline is wrong here 
  newAuthority AS account_owner,
  null as account_mint,
  call_block_time AS event_time,
  DATE_TRUNC('month', call_block_time) AS block_month,
  SUBSTRING(account_mint, 1, 2) AS token_account_prefix,
  'owner_change' AS event_type,
     CONCAT(
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
  account_closeAccount AS token_account,
  null as account_owner,
  null as account_mint,
  call_block_time AS event_time,
  DATE_TRUNC('month', call_block_time) AS block_month,
  SUBSTRING(account_closeAccount, 1, 2) AS token_account_prefix,
  'close' AS event_type,
  CONCAT(
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