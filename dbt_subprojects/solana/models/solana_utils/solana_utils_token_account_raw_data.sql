{{
  config (
    schema='solana_utils',
    alias='token_account_raw_data',
    partition_by=['block_date'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['block_date', 'block_slot', 'tx_index', 'inner_instruction_index', 'outer_instruction_index', 'token_account'],
    incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

--Init events contain mint address, and owner address
--Init v1
SELECT
  CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
  , call_block_time AS block_time
  , call_block_slot AS block_slot
  , call_tx_index AS tx_index
  , call_inner_instruction_index AS inner_instruction_index
  , call_outer_instruction_index AS outer_instruction_index
  , account_account AS token_account
  , SUBSTRING(account_account, 1, 2) AS token_account_prefix
  , 'init' AS event_type
  , account_owner
  , account_mint
FROM {{ source('spl_token_solana', 'spl_token_call_initializeaccount') }}
WHERE
  1=1
  {% if is_incremental() %}
  AND {{ incremental_predicate('call_block_time') }}
  {% endif %}

UNION ALL

--Init v2
SELECT
  CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
  , call_block_time AS block_time
  , call_block_slot AS block_slot
  , call_tx_index AS tx_index
  , call_inner_instruction_index AS inner_instruction_index
  , call_outer_instruction_index AS outer_instruction_index
  , account_account AS token_account
  , SUBSTRING(account_account, 1, 2) AS token_account_prefix
  , 'init' AS event_type
  , owner as account_owner
  , account_mint
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
  , call_block_slot AS block_slot
  , call_tx_index AS tx_index
  , call_inner_instruction_index AS inner_instruction_index
  , call_outer_instruction_index AS outer_instruction_index
  , account_account AS token_account
  , SUBSTRING(account_account, 1, 2) AS token_account_prefix
  , 'init' AS event_type
  , owner as account_owner
  , account_mint
FROM {{ source('spl_token_solana', 'spl_token_call_initializeaccount3') }}
WHERE
  1=1
  {% if is_incremental() %}
  AND {{ incremental_predicate('call_block_time') }}
  {% endif %}

UNION ALL


-- Owner Changes: Only owner changes, mint persists, mint address not in data
SELECT
  CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
  , call_block_time AS block_time
  , call_block_slot AS block_slot
  , call_tx_index AS tx_index
  , call_inner_instruction_index AS inner_instruction_index
  , call_outer_instruction_index AS outer_instruction_index
  , account_owned AS token_account
  , SUBSTRING(account_owned, 1, 2) AS token_account_prefix
  , 'owner_change' AS event_type
  , newAuthority as account_owner
  , null as account_mint
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
  , call_block_slot AS block_slot
  , call_tx_index AS tx_index
  , call_inner_instruction_index AS inner_instruction_index
  , call_outer_instruction_index AS outer_instruction_index
  , account_account AS token_account
  , SUBSTRING(account_account, 1, 2) AS token_account_prefix
  , 'close' AS event_type
  , null as account_owner
  , null as account_mint
  FROM {{ source('spl_token_solana', 'spl_token_call_closeaccount') }}
  WHERE 
    1=1
    {% if is_incremental() %}
    AND {{ incremental_predicate('call_block_time') }}
    {% endif %}