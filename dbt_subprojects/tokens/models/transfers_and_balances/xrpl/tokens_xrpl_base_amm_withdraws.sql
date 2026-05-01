{{
  config(
    schema = 'tokens_xrpl',
    alias = 'base_amm_withdraws',
    materialized = 'view',
  )
}}

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
where transaction_type = 'AMMWithdraw'
  and transfer_type in ('amm_withdraw', 'amm_lp_burn')
