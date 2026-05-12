{{
  config(
    schema = 'tokens_xrpl',
    alias = 'base_payments',
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

with payment_transactions as (
  select
    t.tx_hash,
    t.blockchain,
    t.block_month,
    t.block_date,
    t.block_time,
    t.block_number,
    t.tx_from,
    t.tx_to,
    t.tx_index,
    t.transaction_type,
    t.transaction_result,
    t.flags,
    coalesce(t.delivered_currency, t.amount_currency, t.deliver_max_currency) as currency,
    coalesce(t.delivered_issuer, t.amount_issuer, t.deliver_max_issuer) as issuer_raw,
    coalesce(t.delivered_value, t.amount_value, t.deliver_max_value) as amount_value
  from {{ ref('tokens_xrpl_transaction_metadata') }} t
  where t.block_date >= date '{{ xrpl_transfer_start_date }}'
    and t.transaction_type = 'Payment'
    and t.transaction_result = 'tesSUCCESS'
    and t.tx_to is not null
    {% if is_incremental() -%}
    and {{ incremental_predicate('t.block_date') }}
    {% endif -%}
),

normalized_transfers as (
  select
    {{ dbt_utils.generate_surrogate_key(['tx_hash']) }} as unique_key,
    blockchain,
    block_month,
    block_date,
    block_time,
    block_number,
    tx_hash,
    case
      when currency = 'XRP' then 'native'
      else 'issued'
    end as token_standard,
    tx_from,
    tx_to,
    tx_index,
    tx_from as "from",
    tx_to as to,
    case
      when currency = 'XRP' then 'rrrrrrrrrrrrrrrrrrrrrhoLvTp'
      else issuer_raw
    end as issuer,
    currency,
    case
      when length(currency) = 40 then upper(currency)
      else cast(null as varchar)
    end as currency_hex,
    try_cast(amount_value as double) as amount_raw,
    case
      when currency = 'XRP' then try_cast(amount_value as double) / 1000000.0
      else try_cast(amount_value as double)
    end as amount,
    'payment' as transfer_type,
    transaction_type,
    transaction_result,
    case
      when coalesce(bitwise_and(flags, 131072), 0) = 131072 then true
      else false
    end as partial_payment_flag,
    current_timestamp as _updated_at
  from payment_transactions
)

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
  case
    when currency = 'XRP' then 'xrp'
    when issuer is null or currency is null then cast(null as varchar)
    when currency_hex is not null then concat(lower(issuer), ':', lower(currency_hex))
    else concat(lower(issuer), ':', lower(currency))
  end as xrpl_asset_id,
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
from normalized_transfers
where amount_raw > 0
