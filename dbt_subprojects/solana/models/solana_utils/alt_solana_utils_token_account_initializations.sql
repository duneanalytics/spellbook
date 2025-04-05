{{
  config(
    schema='solana_utils',
    alias='token_account_initializations',
    materialized='incremental',
    file_format='delta',
    partition_by=['token_account_prefix', 'block_month'],
    incremental_strategy='merge',
    incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.event_time')],
    unique_key=['token_account', 'instruction_uniq_id']
  )
}}

{% set start_date = '2021-01-01' %}

-- Collect all initialization events from different sources
SELECT
  account_account AS token_account,
  account_owner,
  account_mint,
  call_block_time AS event_time,
  DATE_TRUNC('month', call_block_time) AS block_month,
  SUBSTRING(account_account, 1, 1) AS token_account_prefix,
  CONCAT(
      CAST(call_block_slot AS VARCHAR), '-', 
      CAST(call_tx_index AS VARCHAR), '-', 
      CAST(COALESCE(call_outer_instruction_index,0) AS VARCHAR), '-', 
      CAST(COALESCE(call_inner_instruction_index, 0) AS VARCHAR)
    ) AS instruction_uniq_id
FROM {{ source('spl_token_solana', 'spl_token_call_initializeaccount') }}
WHERE call_block_time >= timestamp '{{start_date}}'
{% if is_incremental() %}
  AND {{ incremental_predicate('call_block_time') }}
{% endif %}

UNION ALL

SELECT
  account_account,
  owner as account_owner,
  account_mint,
  call_block_time AS event_time,
  DATE_TRUNC('month', call_block_time) AS block_month,
  SUBSTRING(account_account, 1, 1) AS token_account_prefix,
  CONCAT(
      CAST(call_block_slot AS VARCHAR), '-', 
      CAST(call_tx_index AS VARCHAR), '-', 
      CAST(COALESCE(call_outer_instruction_index,0) AS VARCHAR), '-', 
      CAST(COALESCE(call_inner_instruction_index, 0) AS VARCHAR)
    ) AS instruction_uniq_id
FROM {{ source('spl_token_solana', 'spl_token_call_initializeaccount2') }}
WHERE call_block_time >= timestamp '{{start_date}}'
{% if is_incremental() %}
  AND {{ incremental_predicate('call_block_time') }}
{% endif %}

UNION ALL

SELECT
  account_account,
  owner as account_owner,
  account_mint,
  call_block_time AS event_time,
  DATE_TRUNC('month', call_block_time) AS block_month,
  SUBSTRING(account_account, 1, 1) AS token_account_prefix,
  CONCAT(
      CAST(call_block_slot AS VARCHAR), '-', 
      CAST(call_tx_index AS VARCHAR), '-', 
      CAST(COALESCE(call_outer_instruction_index,0) AS VARCHAR), '-', 
      CAST(COALESCE(call_inner_instruction_index, 0) AS VARCHAR)
    ) AS instruction_uniq_id
FROM {{ source('spl_token_solana', 'spl_token_call_initializeaccount3') }}
WHERE call_block_time >= timestamp '{{start_date}}'
{% if is_incremental() %}
  AND {{ incremental_predicate('call_block_time') }}
{% endif %} 