-- EIP-7708 native reconciliation for tokens_arc_base_transfers.
--
-- Purpose:
-- The EIP-7708 system emitter is the only native source on Arc: every native USDC movement
-- logs there, and the chain's 6-decimal ERC-20 interface is excluded because the emitter
-- already carries those same movements. Verify the model's native rows are exactly that
-- emitter stream -- one row per log, no more -- with counterparties and amount unchanged.
--
-- The unique_key uniqueness test cannot see this: an interface row that re-entered would
-- carry its own event index and so would not collide with its emitter sibling.
--
-- Failure interpretation:
-- A null actual_tx_hash means an emitter log was dropped; a null expected_tx_hash means a
-- native row exists with no emitter log behind it (a re-entered interface row, or a
-- misclassification); anything else is field drift.

with model_coverage as (
  -- The model is a snapshot taken before this test runs, so blocks landing in between
  -- would otherwise read as dropped rows. Bound the source at the model's own coverage.
  -- A build is one consistent read, so every row at or below this block is present.
  select max(block_number) as max_block_number
  from {{ ref('tokens_arc_base_transfers') }}
  where {{ incremental_predicate('block_time') }}
),

expected_rows as (
  select
    t.evt_tx_hash as tx_hash,
    cast(t.evt_index as bigint) as evt_index,
    t."from" as "from",
    t.to as to,
    t.value as amount_raw
  from {{ source('erc20_arc', 'evt_Transfer') }} t
  inner join {{ source('arc', 'transactions') }} tx
    on tx.block_date = cast(date_trunc('day', t.evt_block_time) as date)
    and tx.block_number = t.evt_block_number
    and tx.hash = t.evt_tx_hash
  where t.contract_address = 0xfffffffffffffffffffffffffffffffffffffffe
    and t.evt_block_number <= (select max_block_number from model_coverage)
    and {{ incremental_predicate('t.evt_block_time') }}
    and {{ incremental_predicate('tx.block_time') }}
),

actual_rows as (
  select
    tx_hash,
    evt_index,
    "from",
    to,
    amount_raw
  from {{ ref('tokens_arc_base_transfers') }}
  where token_standard = 'native'
    and {{ incremental_predicate('block_time') }}
)

select
  coalesce(e.tx_hash, a.tx_hash) as tx_hash,
  coalesce(e.evt_index, a.evt_index) as evt_index,
  e.tx_hash as expected_tx_hash,
  a.tx_hash as actual_tx_hash,
  e.amount_raw as expected_amount_raw,
  a.amount_raw as actual_amount_raw
from expected_rows e
full outer join actual_rows a
  on e.tx_hash = a.tx_hash
  and e.evt_index = a.evt_index
where e.tx_hash is null
  or a.tx_hash is null
  or e."from" is distinct from a."from"
  or e.to is distinct from a.to
  or e.amount_raw is distinct from a.amount_raw
