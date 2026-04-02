{{
  config(
    schema = 'tokens_aptos',
    alias = 'transfer_pairs',
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

with transfer_events as (
  select *
  from {{ ref('tokens_aptos_transfer_events') }}
  where block_date >= date '{{ aptos_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %}
),

withdraw_events as (
  select
    tx_version,
    tx_hash,
    block_date,
    block_time,
    block_month,
    event_index,
    owner_address,
    storage_id,
    asset_type,
    token_standard,
    amount_raw,
    row_number() over (
      partition by tx_version, asset_type, amount_raw
      order by event_index
    ) as pair_rank
  from transfer_events
  where transfer_direction = 'debit'
),

deposit_events as (
  select
    tx_version,
    tx_hash,
    block_date,
    block_time,
    block_month,
    event_index,
    owner_address,
    storage_id,
    asset_type,
    token_standard,
    amount_raw,
    row_number() over (
      partition by tx_version, asset_type, amount_raw
      order by event_index
    ) as pair_rank
  from transfer_events
  where transfer_direction = 'credit'
)

select
  {{ dbt_utils.generate_surrogate_key(['w.tx_version', 'w.event_index', 'd.event_index']) }} as unique_key,
  w.tx_version,
  w.tx_hash,
  w.block_date,
  w.block_time,
  w.block_month,
  w.event_index as withdraw_event_index,
  d.event_index as deposit_event_index,
  w.owner_address as from_address,
  d.owner_address as to_address,
  w.storage_id as from_storage_id,
  d.storage_id as to_storage_id,
  w.asset_type,
  w.token_standard,
  w.amount_raw,
  'transfer' as transfer_type,
  current_timestamp as _updated_at
from withdraw_events w
inner join deposit_events d
  on w.tx_version = d.tx_version
  and w.asset_type = d.asset_type
  and w.amount_raw = d.amount_raw
  and w.pair_rank = d.pair_rank
