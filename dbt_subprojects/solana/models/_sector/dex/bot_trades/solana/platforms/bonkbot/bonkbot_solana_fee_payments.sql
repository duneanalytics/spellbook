{{ config(
    materialized = 'table',
    schema = 'bonkbot_solana',
    alias = 'fee_payments',
    partition_by = ['block_month'],
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['tx_id', 'fee_token_mint_address']
   )
}}

{% set bot_label = 'BonkBot' %}
{% set blockchain = 'solana' %}
{% set project_start_date = '2023-08-17' %}
{% set fee_receiver = 'ZG98FUCjb8mJ824Gbs6RsgVmr1FhXb2oNiJHa2dwmPd' %}
{% set wsol_token = 'So11111111111111111111111111111111111111112' %}

WITH
fee_addresses AS (
  SELECT 
    '{{fee_receiver}}' AS fee_receiver
  ),
  fee_payments AS (
    SELECT
      block_time,
      CAST(date_trunc('month', block_time) AS date) AS block_month,
      fee_receiver,
      IF(
        balance_change > 0,
        balance_change / 1e9,
        token_balance_change
      ) AS fee_token_amount,
      IF(
        balance_change > 0,
        '{{wsol_token}}',
        token_mint_address
      ) AS fee_token_mint_address,
      tx_id
    FROM
      {{ source('solana','account_activity') }} as account_activity
      JOIN fee_addresses ON ((fee_addresses.fee_receiver = account_activity.address AND balance_change > 0) OR
      (token_balance_owner = fee_addresses.fee_receiver AND token_balance_change > 0))
    WHERE
      {% if is_incremental() %}
      {{ incremental_predicate('block_time') }}
      {% else %}
      block_time >= TIMESTAMP '{{project_start_date}}'
      {% endif %}
      AND tx_success
  ),
  -- Eliminate duplicates (e.g. both SOL + WSOL in a single transaction)
  aggregated_fee_payments_by_token_by_tx AS (
    SELECT
      tx_id,
      fee_token_mint_address,
      block_time,
      block_month,
      SUM(fee_token_amount) AS fee_token_amount
    FROM
      fee_payments
    GROUP BY
      tx_id,
      fee_token_mint_address,
      block_time,
      block_month
  )
SELECT 
   tx_id,
   '{{bot_label}}' as bot,
   '{{blockchain}}' as blockchain,
   fee_token_mint_address,
   block_time,
   block_month,
   fee_token_amount,
   ROW_NUMBER() OVER (PARTITION BY fee_token_mint_address, tx_id ORDER BY block_time) as index
FROM
  aggregated_fee_payments_by_token_by_tx