{{
  config(
    schema = 'tokens_xrpl',
    alias = 'transfers',
    materialized = 'incremental',
    file_format = 'delta',
    partition_by = ['block_month'],
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    merge_skip_unchanged = true,
  )
}}

{% set xrpl_transfer_start_date = '2026-01-01' %} -- ci test, revert to '2012-06-01'

with base_transfers as (
  select
    *
  from {{ ref('tokens_xrpl_base_transfers') }}
  where block_date >= date '{{ xrpl_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('block_date') }}
    {% endif %}
),

-- prices not available for xrpl yet
/*
prices as (
  select
    p.timestamp,
    p.price
  from {{ source('prices_external', 'hour') }} p
  where p.blockchain = 'xrpl'
    and p.timestamp >= timestamp '{{ xrpl_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('p.timestamp') }}
    {% endif %}
),
*/

currency_mapping as (
  select
    currency_hex,
    symbol
  from {{ ref('tokens_xrpl_currency_mapping') }}
),

final as (
  select
    b.unique_key,
    b.blockchain,
    b.block_month,
    b.block_date,
    b.block_time,
    b.block_number,
    b.tx_hash,
    b.token_standard,
    b.tx_from,
    b.tx_to,
    b.tx_index,
    b."from",
    b.to,
    b.xrpl_asset_id,
    b.issuer,
    b.currency,
    b.currency_hex,
    case
      when b.currency = 'XRP' then 'XRP'
      when b.currency_hex is not null and m.symbol is not null then m.symbol
      when b.currency_hex is not null then substr(b.currency_hex, 1, 8)
      else b.currency
    end as symbol,
    case
      when b.currency = 'XRP' then 6
      else cast(null as integer)
    end as decimals,
    b.amount_raw,
    b.amount,
    cast(null as double) as price_usd,
    cast(null as double) as amount_usd,
    b.transfer_type,
    b.transaction_type,
    b.transaction_result,
    b.partial_payment_flag,
    b._updated_at
  from base_transfers b
  left join currency_mapping m
    on b.currency_hex = m.currency_hex
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
  xrpl_asset_id,
  issuer,
  currency,
  currency_hex,
  symbol,
  decimals,
  amount_raw,
  amount,
  price_usd,
  amount_usd,
  transfer_type,
  transaction_type,
  transaction_result,
  partial_payment_flag,
  _updated_at
from final
