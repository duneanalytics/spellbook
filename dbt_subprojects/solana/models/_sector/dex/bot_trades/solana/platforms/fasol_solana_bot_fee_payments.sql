{{ config(
    alias = 'fee_payments',
    schema = 'fasol_solana',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['tx_id', 'fee_token_amount', 'fee_token_mint_address']
   )
}}

{% set project_start_date = '2024-06-15' %}
{% set fee_receiver_1 = 'HCXaTnCeufUNUwS63B67dKbvfX6cNw49H3H6EWEriTuA' %}
{% set wsol_token = 'So11111111111111111111111111111111111111112' %}

SELECT
  tx_id,
  block_time,
  'SOL' AS feeTokenType,
  balance_change / 1e9 AS fee_token_amount,
  '{{wsol_token}}' AS fee_token_mint_address
FROM
  {{ source('solana','account_activity') }}
WHERE
  {% if is_incremental() %}
  {{ incremental_predicate('block_time') }}
  {% else %}
  block_time >= TIMESTAMP '{{project_start_date}}'
  {% endif %}
  AND tx_success
  AND balance_change > 0
  AND (
    address = '{{fee_receiver_1}}'
    )
