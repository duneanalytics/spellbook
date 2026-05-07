{% set blockchain = 'tezos_evm' %}

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
    txns.block_number,
    txns.hash as tx_hash,
    txns."index" as tx_index,
    txns."from" as tx_from,
    txns.to as tx_to,
    cast(gas_price as uint256) as gas_price,
    txns.gas_used as gas_used,
    cast(gas_price as uint256) * cast(txns.gas_used as uint256) as tx_fee_raw,
    map_concat(
      map(),
      case
        when txns.priority_fee_per_gas is null or txns.priority_fee_per_gas < 0
          then map(array['base_fee'], array[(cast(gas_price as uint256) * cast(txns.gas_used as uint256))])
        else map(
          array['base_fee', 'priority_fee', 'inclusion_fee'],
          array[
            (cast(coalesce(blocks.base_fee_per_gas, 0) as uint256) * cast(txns.gas_used as uint256)),
            (cast(txns.priority_fee_per_gas as uint256) * cast(txns.gas_used as uint256)),
            case
              when cast(txns.gas_price as uint256) > cast(coalesce(blocks.base_fee_per_gas, 0) as uint256) + cast(txns.priority_fee_per_gas as uint256)
                then (cast(txns.gas_price as uint256) - cast(coalesce(blocks.base_fee_per_gas, 0) as uint256) - cast(txns.priority_fee_per_gas as uint256)) * cast(txns.gas_used as uint256)
              else uint256 '0'
            end
          ]
        )
      end
    ) as tx_fee_breakdown_raw,
    blocks.miner as block_proposer,
    txns.max_fee_per_gas,
    txns.priority_fee_per_gas,
    txns.max_priority_fee_per_gas,
    blocks.base_fee_per_gas,
    txns.gas_limit,
    case
      when txns.gas_limit = 0 then null
      when txns.gas_limit != 0 then cast(txns.gas_used as double) / cast(txns.gas_limit as double)
    end as gas_limit_usage
  from {{ source(blockchain, 'transactions') }} as txns
  inner join {{ source(blockchain, 'blocks') }} as blocks
    on txns.block_number = blocks.number
    {% if is_incremental() -%}
    and {{ incremental_predicate('blocks.time') }}
    {% endif %}
  {% if is_incremental() -%}
  where {{ incremental_predicate('txns.block_time') }}
  {% endif %}
)

select
  '{{blockchain}}' as blockchain,
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
  coalesce(b.tx_fee_raw, 0) as tx_fee_raw,
  coalesce(b.tx_fee_raw, 0) / pow(10, p.decimals) as tx_fee,
  coalesce(b.tx_fee_raw, 0) / pow(10, p.decimals) * p.price as tx_fee_usd,
  transform_values(
    b.tx_fee_breakdown_raw,
    (k, v) -> coalesce(v, 0)
  ) as tx_fee_breakdown_raw,
  transform_values(
    b.tx_fee_breakdown_raw,
    (k, v) -> coalesce(v, 0) / pow(10, p.decimals)
  ) as tx_fee_breakdown,
  transform_values(
    b.tx_fee_breakdown_raw,
    (k, v) -> coalesce(v, 0) / pow(10, p.decimals) * p.price
  ) as tx_fee_breakdown_usd,
  p.contract_address as tx_fee_currency,
  b.block_proposer,
  b.max_fee_per_gas,
  b.priority_fee_per_gas,
  b.max_priority_fee_per_gas,
  b.base_fee_per_gas,
  b.gas_limit,
  b.gas_limit_usage
from base_model as b
left join native_token_prices as p on p.timestamp = date_trunc('day', b.block_time)
