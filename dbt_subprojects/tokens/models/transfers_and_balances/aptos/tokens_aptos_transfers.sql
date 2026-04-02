{{
  config(
    schema = 'tokens_aptos',
    alias = 'transfers',
    materialized = 'incremental',
    file_format = 'delta',
    partition_by = ['block_month'],
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    merge_skip_unchanged = true,
    post_hook = '{{ hide_spells() }}'
  )
}}

{% set aptos_transfer_start_date = '2026-01-01' %} -- ci test only

with base_transfers as (
  select *
  from {{ ref('tokens_aptos_base_transfers') }}
  where block_date >= date '{{ aptos_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %}
),

tx_metadata as (
  select
    t.version as tx_version,
    t.tx_index,
    from_hex(
      '0x' || lpad(ltrim(t.sender, '0x'), 64, '0')
    ) as tx_from
  from {{ source('aptos', 'user_transactions') }} t
  where t.block_timestamp >= timestamp '{{ aptos_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('t.block_timestamp') }}
    {% endif %}
),

asset_metadata as (
  select
    m.asset_type,
    m.asset_symbol,
    m.decimals
  from {{ source('aptos_fungible_asset', 'metadata_current') }} m
),

prices as (
  select
    p.timestamp,
    p.contract_address,
    p.price
  from {{ source('prices_external', 'hour') }} p
  where p.blockchain = 'aptos'
    {% if is_incremental() %}
    and {{ incremental_predicate('p.timestamp') }}
    {% endif %}
),

final as (
  select
    b.unique_key,
    'aptos' as blockchain,
    b.block_month,
    b.block_date,
    b.block_time,
    b.tx_version,
    b.tx_hash,
    b.event_index,
    b.counterpart_event_index,
    b.token_standard,
    tx.tx_from,
    cast(null as varbinary) as tx_to,
    b.from_address as "from",
    b.to_address as "to",
    b.contract_address,
    b.asset_type,
    b.from_storage_id,
    b.to_storage_id,
    m.asset_symbol as symbol,
    m.decimals,
    tx.tx_index,
    b.amount_raw,
    cast(b.amount_raw as double) / power(10, cast(m.decimals as double)) as amount,
    p.price as price_usd,
    cast(b.amount_raw as double) / power(10, cast(m.decimals as double)) * p.price as amount_usd,
    b.transfer_type,
    b._updated_at
  from base_transfers b
  left join asset_metadata m
    on b.asset_type = m.asset_type
  left join tx_metadata tx
    on b.tx_version = tx.tx_version
  left join prices p
    on date_trunc('hour', b.block_time) = p.timestamp
    and b.contract_address = p.contract_address
)

select
  unique_key,
  blockchain,
  block_month,
  block_date,
  block_time,
  tx_version,
  tx_hash,
  event_index,
  counterpart_event_index,
  token_standard,
  tx_from,
  tx_to,
  "from",
  "to",
  contract_address,
  asset_type,
  from_storage_id,
  to_storage_id,
  symbol,
  decimals,
  tx_index,
  amount_raw,
  amount,
  price_usd,
  amount_usd,
  transfer_type,
  _updated_at
from final
