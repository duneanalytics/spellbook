-- Reconciliation invariant for Aptos event consumption.
--
-- Purpose:
-- Verify that, for each recent (tx_version, event_index) in transfer_events,
-- the total amount represented by base_transfers exactly matches the source
-- event amount after pairing and residual shaping.
--
-- Failure interpretation:
-- Any returned row means paired rows plus residual rows do not fully account for
-- the source event, which indicates missing, extra, or mis-sized transfer legs.

with event_scope as (
  select
    e.tx_version,
    e.event_index,
    cast(e.amount_raw as decimal(38, 0)) as expected_amount_raw
  from {{ ref('tokens_aptos_transfer_events') }} e
  where {{ incremental_predicate('e.block_time') }}
),

actual_event_amounts_raw as (
  select
    b.tx_version,
    b.event_index,
    cast(b.amount_raw as decimal(38, 0)) as represented_amount_raw
  from {{ ref('tokens_aptos_base_transfers') }} b
  where {{ incremental_predicate('b.block_time') }}
    and b.event_index is not null
  union all
  select
    b.tx_version,
    b.counterpart_event_index as event_index,
    cast(b.amount_raw as decimal(38, 0)) as represented_amount_raw
  from {{ ref('tokens_aptos_base_transfers') }} b
  where {{ incremental_predicate('b.block_time') }}
    and b.counterpart_event_index is not null
),

actual_event_amounts as (
  select
    tx_version,
    event_index,
    sum(represented_amount_raw) as actual_amount_raw
  from actual_event_amounts_raw
  group by 1, 2
)

select
  coalesce(e.tx_version, a.tx_version) as tx_version,
  coalesce(e.event_index, a.event_index) as event_index,
  e.expected_amount_raw,
  a.actual_amount_raw
from event_scope e
full outer join actual_event_amounts a
  on e.tx_version = a.tx_version
  and e.event_index = a.event_index
where coalesce(e.expected_amount_raw, cast(0 as decimal(38, 0)))
  != coalesce(a.actual_amount_raw, cast(0 as decimal(38, 0)))
