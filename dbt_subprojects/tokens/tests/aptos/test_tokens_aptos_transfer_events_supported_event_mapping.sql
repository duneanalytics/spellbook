-- Mapping invariant for Aptos transfer-event normalization.
--
-- Purpose:
-- Ensure supported Aptos deposit/withdraw events always map to the expected
-- normalized activity and direction labels, and reject unsupported event types.
--
-- Failure interpretation:
-- Any returned row means transfer-event normalization drifted or upstream began
-- surfacing an event type that this lineage does not intentionally model.

select
  e.block_date,
  e.tx_version,
  e.event_index,
  e.event_type,
  e.activity_type,
  e.transfer_direction
from {{ ref('tokens_aptos_transfer_events') }} e
where {{ incremental_predicate('e.block_time') }}
  and (
    e.event_type not in (
      '0x1::coin::WithdrawEvent',
      '0x1::fungible_asset::WithdrawEvent',
      '0x1::fungible_asset::Withdraw',
      '0x1::coin::DepositEvent',
      '0x1::fungible_asset::DepositEvent',
      '0x1::fungible_asset::Deposit'
    )
    or (
      e.event_type in (
        '0x1::coin::WithdrawEvent',
        '0x1::fungible_asset::WithdrawEvent',
        '0x1::fungible_asset::Withdraw'
      )
      and (
        e.activity_type is distinct from 'withdraw'
        or e.transfer_direction is distinct from 'debit'
      )
    )
    or (
      e.event_type in (
        '0x1::coin::DepositEvent',
        '0x1::fungible_asset::DepositEvent',
        '0x1::fungible_asset::Deposit'
      )
      and (
        e.activity_type is distinct from 'deposit'
        or e.transfer_direction is distinct from 'credit'
      )
    )
  )
