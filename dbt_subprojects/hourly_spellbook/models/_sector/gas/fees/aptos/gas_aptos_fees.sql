{% set blockchain = 'aptos' %}

{{
  config(
    schema = 'gas_' + blockchain,
    alias = 'fees',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'tx_hash'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

with

native_token_prices as (
  {{ native_token_prices(blockchain) }}
),

base_model as (
  select
    txns.block_time,
    txns.block_height as block_number,
    txns.hash as tx_hash,
    txns.tx_index,
    from_hex(lpad(to_hex(txns.sender), 64, '0')) as tx_from,
    cast(null as varbinary) as tx_to,
    cast(txns.gas_unit_price as uint256) as gas_price,
    cast(txns.gas_used as uint256) as gas_used,
    cast(txns.gas_unit_price as uint256) * cast(txns.gas_used as uint256) as tx_fee_raw,
    map(
      array['total_fee'],
      array[cast(txns.gas_unit_price as uint256) * cast(txns.gas_used as uint256)]
    ) as tx_fee_breakdown_raw,
    txns.block_proposer,
    cast(txns.max_gas_amount as uint256) as gas_limit,
    case
      when cast(txns.max_gas_amount as uint256) = uint256 '0' then null
      else cast(txns.gas_used as double) / cast(txns.max_gas_amount as double)
    end as gas_limit_usage
  from {{ source(blockchain, 'user_transactions') }} txns
  where txns.block_time >= timestamp '2022-10-12'
    {% if is_incremental() %}
    and {{ incremental_predicate('txns.block_time') }}
    {% endif %}
)

select
  '{{ blockchain }}' as blockchain,
  cast(date_trunc('month', b.block_time) as date) as block_month,
  cast(date_trunc('day', b.block_time) as date) as block_date,
  b.block_time,
  b.block_number,
  b.tx_hash,
  b.tx_index,
  b.tx_from,
  b.tx_to,
  b.gas_price,
  b.gas_used,
  p.symbol as currency_symbol,
  coalesce(b.tx_fee_raw, uint256 '0') as tx_fee_raw,
  coalesce(b.tx_fee_raw, uint256 '0') / pow(10, p.decimals) as tx_fee,
  coalesce(b.tx_fee_raw, uint256 '0') / pow(10, p.decimals) * p.price as tx_fee_usd,
  transform_values(
    b.tx_fee_breakdown_raw,
    (k, v) -> coalesce(v, uint256 '0')
  ) as tx_fee_breakdown_raw,
  transform_values(
    b.tx_fee_breakdown_raw,
    (k, v) -> coalesce(v, uint256 '0') / pow(10, p.decimals)
  ) as tx_fee_breakdown,
  transform_values(
    b.tx_fee_breakdown_raw,
    (k, v) -> coalesce(v, uint256 '0') / pow(10, p.decimals) * p.price
  ) as tx_fee_breakdown_usd,
  p.contract_address as tx_fee_currency,
  b.block_proposer,
  b.gas_limit,
  b.gas_limit_usage
from base_model as b
left join native_token_prices as p
  on p.timestamp = date_trunc('day', b.block_time)
