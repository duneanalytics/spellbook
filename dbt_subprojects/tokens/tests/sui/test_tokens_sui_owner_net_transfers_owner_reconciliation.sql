-- Reconciliation invariant for owner-net construction.
--
-- Purpose:
-- Verify that, for each (tx_digest, coin_type, owner) in recent partitions,
-- the signed owner delta implied by `tokens_sui_owner_net_transfers`
-- exactly matches the owner delta reconstructed from `tokens_sui_object_event_deltas`.
--
-- Scope note:
-- For bounded-history CI runs, the test scopes tx+coin coverage to the active
-- `tokens_sui_object_event_deltas` window, then excludes pairs whose first
-- relevant object state depends only on history before `sui_transfer_start_date`.
-- This keeps the invariant focused on the modeled window rather than on
-- intentionally excluded history.
--
-- Failure interpretation:
-- Any returned row means owner-net legs are missing, extra, or mis-sized for that
-- tx+coin+owner tuple (expected delta != actual reconciled delta).

{% set sui_transfer_start_date = '2025-10-01' %} -- just ci test

with

tx_coin_scope_raw as (
  select distinct
    o.tx_digest,
    o.coin_type
  from {{ ref('tokens_sui_owner_net_transfers') }} o
  where {{ incremental_predicate('o.block_date') }}
),

window_object_scope as (
  select distinct
    f.tx_digest,
    f.coin_type,
    f.object_id
  from {{ ref('tokens_sui_object_event_deltas') }} f
  inner join tx_coin_scope_raw s
    on f.tx_digest = s.tx_digest
    and f.coin_type = s.coin_type
  where {{ incremental_predicate('f.block_date') }}
    and f.object_id is not null
),

objects_with_pre_start_history as (
  select distinct
    h.object_id
  from {{ ref('tokens_sui_coin_object_history') }} h
  where h.block_date < date '{{ sui_transfer_start_date }}'
),

objects_with_in_scope_pre_window_history as (
  select distinct
    h.object_id
  from {{ ref('tokens_sui_coin_object_history') }} h
  where h.block_date >= date '{{ sui_transfer_start_date }}'
    and not {{ incremental_predicate('h.block_date') }}
),

tx_coin_out_of_scope_boundary as (
  select distinct
    w.tx_digest,
    w.coin_type
  from window_object_scope w
  inner join objects_with_pre_start_history p
    on w.object_id = p.object_id
  left join objects_with_in_scope_pre_window_history i
    on w.object_id = i.object_id
  where i.object_id is null
),

tx_coin_scope as (
  select
    s.tx_digest,
    s.coin_type
  from tx_coin_scope_raw s
  left join tx_coin_out_of_scope_boundary b
    on s.tx_digest = b.tx_digest
    and s.coin_type = b.coin_type
  where b.tx_digest is null
),

owner_true_delta as (
  select
    f.tx_digest,
    f.coin_type,
    f.receiver as owner,
    cast(f.coin_balance as decimal(38, 0)) as owner_delta_raw
  from {{ ref('tokens_sui_object_event_deltas') }} f
  inner join tx_coin_scope s
    on f.tx_digest = s.tx_digest
    and f.coin_type = s.coin_type
  where {{ incremental_predicate('f.block_date') }}
    and f.object_status = 'Created'
    and f.receiver is not null
    and f.coin_balance is not null
  union all
  select
    f.tx_digest,
    f.coin_type,
    f.prev_owner as owner,
    cast(-f.prev_balance as decimal(38, 0)) as owner_delta_raw
  from {{ ref('tokens_sui_object_event_deltas') }} f
  inner join tx_coin_scope s
    on f.tx_digest = s.tx_digest
    and f.coin_type = s.coin_type
  where {{ incremental_predicate('f.block_date') }}
    and f.object_status = 'Deleted'
    and f.prev_owner is not null
    and f.prev_balance is not null
  union all
  select
    f.tx_digest,
    f.coin_type,
    f.prev_owner as owner,
    cast(-f.prev_balance as decimal(38, 0)) as owner_delta_raw
  from {{ ref('tokens_sui_object_event_deltas') }} f
  inner join tx_coin_scope s
    on f.tx_digest = s.tx_digest
    and f.coin_type = s.coin_type
  where {{ incremental_predicate('f.block_date') }}
    and f.object_status = 'Mutated'
    and f.has_ownership_change
    and f.prev_owner is not null
    and f.prev_balance is not null
  union all
  select
    f.tx_digest,
    f.coin_type,
    f.receiver as owner,
    cast(f.coin_balance as decimal(38, 0)) as owner_delta_raw
  from {{ ref('tokens_sui_object_event_deltas') }} f
  inner join tx_coin_scope s
    on f.tx_digest = s.tx_digest
    and f.coin_type = s.coin_type
  where {{ incremental_predicate('f.block_date') }}
    and f.object_status = 'Mutated'
    and f.has_ownership_change
    and f.receiver is not null
    and f.coin_balance is not null
  union all
  select
    f.tx_digest,
    f.coin_type,
    f.receiver as owner,
    cast(f.balance_delta as decimal(38, 0)) as owner_delta_raw
  from {{ ref('tokens_sui_object_event_deltas') }} f
  inner join tx_coin_scope s
    on f.tx_digest = s.tx_digest
    and f.coin_type = s.coin_type
  where {{ incremental_predicate('f.block_date') }}
    and f.object_status = 'Mutated'
    and not f.has_ownership_change
    and f.receiver is not null
    and f.balance_delta != 0
),

owner_true_net as (
  select
    tx_digest,
    coin_type,
    owner,
    sum(owner_delta_raw) as expected_owner_net_delta_raw
  from owner_true_delta
  group by 1, 2, 3
),

owner_net_delta as (
  select
    o.tx_digest,
    o.coin_type,
    o.transfer_from as owner,
    cast(-o.amount_raw as decimal(38, 0)) as owner_delta_raw
  from {{ ref('tokens_sui_owner_net_transfers') }} o
  inner join tx_coin_scope s
    on o.tx_digest = s.tx_digest
    and o.coin_type = s.coin_type
  where {{ incremental_predicate('o.block_date') }}
    and o.transfer_from is not null
    and o.amount_raw != 0
  union all
  select
    o.tx_digest,
    o.coin_type,
    o.transfer_to as owner,
    cast(o.amount_raw as decimal(38, 0)) as owner_delta_raw
  from {{ ref('tokens_sui_owner_net_transfers') }} o
  inner join tx_coin_scope s
    on o.tx_digest = s.tx_digest
    and o.coin_type = s.coin_type
  where {{ incremental_predicate('o.block_date') }}
    and o.transfer_to is not null
    and o.amount_raw != 0
),

owner_net_reconciled as (
  select
    tx_digest,
    coin_type,
    owner,
    sum(owner_delta_raw) as actual_owner_net_delta_raw
  from owner_net_delta
  group by 1, 2, 3
)

select
  coalesce(t.tx_digest, n.tx_digest) as tx_digest,
  coalesce(t.coin_type, n.coin_type) as coin_type,
  coalesce(t.owner, n.owner) as owner,
  t.expected_owner_net_delta_raw,
  n.actual_owner_net_delta_raw
from owner_true_net t
full outer join owner_net_reconciled n
  on t.tx_digest = n.tx_digest
  and t.coin_type = n.coin_type
  and t.owner = n.owner
where coalesce(t.expected_owner_net_delta_raw, cast(0 as decimal(38, 0)))
  != coalesce(n.actual_owner_net_delta_raw, cast(0 as decimal(38, 0)))
