-- What it checks: deleted direct-transfer rows keep expected shape in recent partitions:
-- transfer_to stays null, transfer_from maps to prev_owner, amount_raw matches abs(balance_delta),
-- and ownership-change flag remains false.

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
