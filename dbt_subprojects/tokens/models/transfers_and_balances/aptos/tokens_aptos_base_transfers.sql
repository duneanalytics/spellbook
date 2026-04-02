{{
  config(
    schema = 'tokens_aptos',
    alias = 'base_transfers',
    materialized = 'incremental',
    file_format = 'delta',
    partition_by = ['block_month'],
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    merge_skip_unchanged = true
  )
}}

{% set aptos_transfer_start_date = '2026-01-01' %} -- ci test only

with paired_transfers as (
  select
    p.unique_key,
    p.tx_version,
    p.tx_hash,
    p.block_date,
    p.block_time,
    p.block_month,
    p.withdraw_event_index as event_index,
    p.deposit_event_index as counterpart_event_index,
    p.from_address,
    p.to_address,
    p.from_storage_id,
    p.to_storage_id,
    p.asset_type,
    case
      when split_part(p.asset_type, '::', 1) is null then cast(null as varbinary)
      else from_hex(
        '0x' || lpad(
          ltrim(split_part(p.asset_type, '::', 1), '0x'),
          64,
          '0'
        )
      )
    end as contract_address,
    p.token_standard,
    p.amount_raw,
    p.transfer_type,
    p._updated_at
  from {{ ref('tokens_aptos_transfer_pairs') }} p
  where p.block_date >= date '{{ aptos_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('p.block_time') }}
    {% endif %}
),

residual_transfers as (
  select
    r.unique_key,
    r.tx_version,
    r.tx_hash,
    r.block_date,
    r.block_time,
    r.block_month,
    r.event_index,
    cast(null as bigint) as counterpart_event_index,
    r.from_address,
    r.to_address,
    r.from_storage_id,
    r.to_storage_id,
    r.asset_type,
    case
      when split_part(r.asset_type, '::', 1) is null then cast(null as varbinary)
      else from_hex(
        '0x' || lpad(
          ltrim(split_part(r.asset_type, '::', 1), '0x'),
          64,
          '0'
        )
      )
    end as contract_address,
    r.token_standard,
    r.amount_raw,
    r.transfer_type,
    r._updated_at
  from {{ ref('tokens_aptos_transfer_residuals') }} r
  where r.block_date >= date '{{ aptos_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('r.block_time') }}
    {% endif %}
)

select
  unique_key,
  tx_version,
  tx_hash,
  block_date,
  block_time,
  block_month,
  event_index,
  counterpart_event_index,
  from_address,
  to_address,
  from_storage_id,
  to_storage_id,
  asset_type,
  contract_address,
  token_standard,
  amount_raw,
  transfer_type,
  _updated_at
from paired_transfers
union all
select
  unique_key,
  tx_version,
  tx_hash,
  block_date,
  block_time,
  block_month,
  event_index,
  counterpart_event_index,
  from_address,
  to_address,
  from_storage_id,
  to_storage_id,
  asset_type,
  contract_address,
  token_standard,
  amount_raw,
  transfer_type,
  _updated_at
from residual_transfers
