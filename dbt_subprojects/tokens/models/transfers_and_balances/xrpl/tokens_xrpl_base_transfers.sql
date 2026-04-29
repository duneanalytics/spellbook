{{
  config(
    schema = 'tokens_xrpl',
    alias = 'base_transfers',
    materialized = 'incremental',
    file_format = 'delta',
    partition_by = ['block_month'],
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    merge_skip_unchanged = true,
  )
}}

{% set base_transfer_models = [
  'tokens_xrpl_base_payments',
  'tokens_xrpl_base_check_cash',
  'tokens_xrpl_base_escrow_finish',
  'tokens_xrpl_base_payment_channel_claims',
  'tokens_xrpl_base_amm_deposits',
  'tokens_xrpl_base_amm_withdraws',
] %}

{% for model in base_transfer_models %}
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
from {{ ref(model) }}
{% if is_incremental() %}
where {{ incremental_predicate('block_date') }}
{% endif %}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
