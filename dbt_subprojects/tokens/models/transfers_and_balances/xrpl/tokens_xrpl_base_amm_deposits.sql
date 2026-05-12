{{
  config(
    schema = 'tokens_xrpl',
    alias = 'base_amm_deposits',
    materialized = 'incremental',
    file_format = 'delta',
    partition_by = ['block_month'],
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    merge_skip_unchanged = true,
  )
}}

{% set xrpl_transfer_start_date = '2013-01-01' %}

select
  unique_key,
  blockchain,
  block_month,
  block_date,
  block_time,
  block_number,
  tx_hash,
  token_standard,
  tx_from,
  tx_to,
  tx_index,
  "from",
  to,
  xrpl_asset_id,
  issuer,
  currency,
  currency_hex,
  amount_raw,
  amount,
  transfer_type,
  transaction_type,
  transaction_result,
  partial_payment_flag,
  _updated_at
from {{ ref('tokens_xrpl_amm_balance_deltas') }}
where block_date >= date '{{ xrpl_transfer_start_date }}'
  and transaction_type = 'AMMDeposit'
  and transfer_type in ('amm_deposit', 'amm_lp_mint')
  {% if is_incremental() -%}
  and {{ incremental_predicate('block_date') }}
  {% endif -%}
