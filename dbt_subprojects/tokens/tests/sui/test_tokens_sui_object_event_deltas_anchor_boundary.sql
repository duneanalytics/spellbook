-- Boundary-anchor invariant for object-event deltas.
--
-- Purpose:
-- Ensure incremental window stitching preserves prior object state when an object
-- has pre-window history and receives in-window events.
--
-- Scope note:
-- The test targets objects that appear in the incremental window and also have
-- history outside that window, then inspects the first in-window event row.
--
-- Invariant:
-- For first in-window rows that are not `Created`, `prev_balance` must be non-null
-- (i.e. an anchor state was successfully carried forward).
--
-- Failure interpretation:
-- Any returned row means anchor reconstruction failed for that object boundary,
-- which can distort downstream deltas/transfer derivation.

{% set sui_transfer_start_date = '2025-10-01' %} -- just ci test

with

window_objects as (
  select distinct
    d.object_id
  from {{ ref('tokens_sui_object_event_deltas') }} d
  where {{ incremental_predicate('d.block_date') }}
),

objects_with_pre_window_history as (
  select distinct
    w.object_id
  from window_objects w
  inner join {{ ref('tokens_sui_coin_object_history') }} h
    on h.object_id = w.object_id
  where h.block_date >= date '{{ sui_transfer_start_date }}'
    and not {{ incremental_predicate('h.block_date') }}
),

first_window_events as (
  select
    d.object_id,
    d.version,
    d.object_status,
    d.prev_balance,
    row_number() over (partition by d.object_id order by d.version) as rn
  from {{ ref('tokens_sui_object_event_deltas') }} d
  inner join objects_with_pre_window_history p
    on d.object_id = p.object_id
  where {{ incremental_predicate('d.block_date') }}
)

select
  object_id,
  version,
  object_status,
  prev_balance
from first_window_events
where rn = 1
  and object_status != 'Created'
  and prev_balance is null
