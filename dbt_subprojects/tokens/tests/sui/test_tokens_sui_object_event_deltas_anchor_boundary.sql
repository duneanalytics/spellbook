-- What it checks: for objects with pre-window history and in-window events, 
-- the first in-window non-Created row must have non-null prev_balance (anchor stitching check).

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
  where not {{ incremental_predicate('h.block_date') }}
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
