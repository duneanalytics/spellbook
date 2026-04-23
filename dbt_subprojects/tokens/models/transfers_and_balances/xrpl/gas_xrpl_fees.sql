{{
  config(
    schema = 'gas_xrpl',
    alias = 'fees',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'tx_hash'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
  )
}}

{% set xrpl_gas_start_date = '2012-06-01' %}

with base_transactions as (
  select
    t.blockchain,
    t.block_month,
    t.block_date,
    t.block_time,
    t.block_number,
    t.tx_hash,
    t.tx_index,
    t.tx_from,
    t.tx_to,
    t.transaction_type,
    t.transaction_result,
    try_cast(t.fee as double) as tx_fee_raw,
    0x0000000000000000000000000000000000000000 as price_contract_address
  from {{ ref('tokens_xrpl_transaction_metadata') }} as t
  where t.block_date >= date '{{ xrpl_gas_start_date }}'
    and t.transaction_type in (
      'Payment',
      'PaymentChannelClaim',
      'CheckCash',
      'AMMDeposit',
      'AMMWithdraw',
      'EscrowFinish'
    )
    and try_cast(t.fee as double) > 0
    {% if is_incremental() %}
    and {{ incremental_predicate('t.block_date') }}
    {% endif %}
),

prices as (
  select
    p.timestamp,
    p.contract_address,
    p.symbol,
    p.price
  from {{ source('prices_external', 'hour') }} as p
  where p.blockchain = 'xrpl'
    and p.timestamp >= timestamp '{{ xrpl_gas_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('p.timestamp') }}
    {% endif %}
)

select
  b.blockchain,
  b.block_month,
  b.block_date,
  b.block_time,
  b.block_number,
  b.tx_hash,
  b.tx_index,
  b.tx_from,
  b.tx_to,
  cast(null as double) as gas_price,
  cast(null as bigint) as gas_used,
  coalesce(
    case
      when b.price_contract_address = 0x0000000000000000000000000000000000000000 then 'XRP'
      else cast(null as varchar)
    end,
    p.symbol
  ) as currency_symbol,
  coalesce(b.tx_fee_raw, 0) as tx_fee_raw,
  coalesce(b.tx_fee_raw, 0) / 1000000.0 as tx_fee,
  coalesce(b.tx_fee_raw, 0) / 1000000.0 * p.price as tx_fee_usd,
  map(array['base_fee'], array[coalesce(b.tx_fee_raw, 0)]) as tx_fee_breakdown_raw,
  map(array['base_fee'], array[coalesce(b.tx_fee_raw, 0) / 1000000.0]) as tx_fee_breakdown,
  map(array['base_fee'], array[coalesce(b.tx_fee_raw, 0) / 1000000.0 * p.price]) as tx_fee_breakdown_usd,
  'rrrrrrrrrrrrrrrrrrrrrhoLvTp' as tx_fee_currency,
  cast(null as varchar) as block_proposer,
  cast(null as bigint) as gas_limit,
  cast(null as double) as gas_limit_usage
from base_transactions as b
left join prices as p
  on date_trunc('hour', b.block_time) = p.timestamp
  and b.price_contract_address = p.contract_address
