{{
  config(
    schema='solana_utils',
    alias='token_account_initializations',
    materialized='incremental',
    file_format='delta',
    partition_by=['token_account_prefix', 'block_month'],
    incremental_strategy='merge',
    incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.event_time')],
    unique_key=['token_account', 'event_time']
  )
}}

{% set start_date = '2025-04-01' %}

-- Collect all initialization events from different sources
SELECT
  account_account AS token_account,
  account_owner,
  account_mint,
  call_block_time AS event_time,
  SUBSTRING(account_account, 1, 1) AS token_account_prefix,
  DATE_TRUNC('month', call_block_time) AS block_month
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
  SUBSTRING(account_account, 1, 1) AS token_account_prefix,
  DATE_TRUNC('month', call_block_time) AS block_month
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
  SUBSTRING(account_account, 1, 1) AS token_account_prefix,
  DATE_TRUNC('month', call_block_time) AS block_month
FROM {{ source('spl_token_solana', 'spl_token_call_initializeaccount3') }}
WHERE call_block_time >= timestamp '{{start_date}}'
{% if is_incremental() %}
  AND {{ incremental_predicate('call_block_time') }}
{% endif %} 