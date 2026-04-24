{{
  config(
    schema = 'tokens_sui',
    alias = 'coin_object_latest_state',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['object_id'],
    merge_skip_unchanged = true,
  )
}}

{% set sui_transfer_start_date = '2026-01-01' %} -- ci test, revert to '2023-04-12'

with

-- keep one latest Coin<T> object state per object_id with block_date strictly
-- before the consumer incremental window (now() - DBT_ENV_INCREMENTAL_TIME).
-- lagging the helper by the consumer's window guarantees helper rows remain
-- valid anchors for tokens_sui_object_event_deltas: every window row in the
-- consumer has a strictly later version than its matching helper row.
source_history as (
  select
    h.object_id,
    h.version,
    h.block_date,
    h.block_month,
    h.timestamp_ms,
    h.checkpoint,
    h.owner_type,
    h.receiver,
    h.coin_type,
    h.coin_balance
  from {{ ref('tokens_sui_coin_object_history') }} h
  where h.block_date >= date '{{ sui_transfer_start_date }}'
    and h.block_date < date_trunc(
      '{{ var("DBT_ENV_INCREMENTAL_TIME_UNIT") }}',
      now() - interval '{{ var("DBT_ENV_INCREMENTAL_TIME") }}' {{ var("DBT_ENV_INCREMENTAL_TIME_UNIT") }}
    )
    {% if is_incremental() %}
    -- bounded catch-up window (2x incremental_time) to cover delayed or retried runs
    and h.block_date >= date_trunc(
      '{{ var("DBT_ENV_INCREMENTAL_TIME_UNIT") }}',
      now() - interval '{{ (var("DBT_ENV_INCREMENTAL_TIME") | int) * 2 }}' {{ var("DBT_ENV_INCREMENTAL_TIME_UNIT") }}
    )
    {% endif %}
),

latest_state as (
  select
    s.object_id,
    max(s.version) as version,
    cast(max_by(s.block_date, s.version) as date) as block_date,
    cast(max_by(s.block_month, s.version) as date) as block_month,
    max_by(s.timestamp_ms, s.version) as timestamp_ms,
    max_by(s.checkpoint, s.version) as checkpoint,
    max_by(s.owner_type, s.version) as owner_type,
    max_by(s.receiver, s.version) as receiver,
    max_by(s.coin_type, s.version) as coin_type,
    max_by(s.coin_balance, s.version) as coin_balance
  from source_history s
  group by 1
)

select
  l.object_id,
  l.version,
  l.block_date,
  l.block_month,
  l.timestamp_ms,
  l.checkpoint,
  l.owner_type,
  l.receiver,
  l.coin_type,
  l.coin_balance,
  current_timestamp as _updated_at
from latest_state l
