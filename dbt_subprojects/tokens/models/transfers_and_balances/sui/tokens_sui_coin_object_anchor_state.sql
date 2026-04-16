{{
  config(
    schema = 'tokens_sui',
    alias = 'coin_object_anchor_state',
    materialized = 'incremental',
    file_format = 'delta',
    partition_by = ['block_month'],
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'object_id'],
    merge_skip_unchanged = true,
  )
}}

{% set sui_transfer_start_date = '2025-04-01' %} -- just ci test

with

-- keep one latest object state per object_id + month for downstream anchor lookups
monthly_object_state as (
  select
    h.block_month,
    h.object_id,
    max(h.version) as version,
    cast(max_by(h.block_date, h.version) as date) as block_date,
    max_by(h.timestamp_ms, h.version) as timestamp_ms,
    max_by(h.checkpoint, h.version) as checkpoint,
    max_by(h.owner_type, h.version) as owner_type,
    max_by(h.receiver, h.version) as receiver,
    max_by(h.coin_type, h.version) as coin_type,
    max_by(h.coin_balance, h.version) as coin_balance
  from {{ ref('tokens_sui_coin_object_history') }} h
  where h.block_date >= date '{{ sui_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('h.block_date') }}
    {% endif %}
  group by 1, 2
)

select
  m.block_month,
  m.object_id,
  m.version,
  m.block_date,
  m.timestamp_ms,
  m.checkpoint,
  m.owner_type,
  m.receiver,
  m.coin_type,
  m.coin_balance,
  current_timestamp as _updated_at
from monthly_object_state m
