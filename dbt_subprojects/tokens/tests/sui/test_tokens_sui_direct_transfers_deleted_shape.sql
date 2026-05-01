-- Shape invariant for deleted direct-transfer rows.
--
-- Purpose:
-- Validate that deleted-object rows in `tokens_sui_direct_transfers` keep the
-- expected direct-transfer representation in recent partitions.
--
-- Invariant:
-- - `transfer_to` remains null (deleted rows are one-sided debit legs),
-- - `transfer_from` matches `prev_owner`,
-- - `amount_raw` equals `abs(balance_delta)`,
-- - `has_ownership_change` remains false.
--
-- Failure interpretation:
-- Any returned row indicates deleted-row shaping drift in direct-transfer output
-- (for example, incorrect side assignment or amount derivation).

select
  d.block_date,
  d.object_id,
  d.version,
  d.object_status,
  d.transfer_from,
  d.transfer_to,
  d.prev_owner,
  d.balance_delta,
  d.amount_raw,
  d.has_ownership_change
from {{ ref('tokens_sui_direct_transfers') }} d
where {{ incremental_predicate('d.block_date') }}
  and d.object_status = 'Deleted'
  and (
    d.transfer_to is not null
    or d.transfer_from is distinct from d.prev_owner
    or d.amount_raw is distinct from abs(d.balance_delta)
    or d.has_ownership_change
  )
