-- Native row semantics for tokens_arc_base_transfers.
--
-- Purpose:
-- Native rows come from the 18-decimal EIP-7708 emitter, not the 6-decimal ERC-20
-- interface, and must carry the chain's canonical native address. The address is compared
-- against dune.blockchains rather than a literal, so the test follows a correction to
-- Arc's native metadata instead of pinning today's value.
--
-- Failure interpretation:
-- native_row_off_canonical_address / erc20_row_on_native_address -- token_standard and
-- contract_address disagree about which asset is native.
-- native_amount_not_18_decimals -- a movement made through the 6-decimal interface is not
-- present as a native row at 18 decimals, which no single decimals value could scale.

with model_coverage as (
  -- The model is a snapshot taken before this test runs, so blocks landing in between
  -- would otherwise read as dropped rows. Bound the source at the model's own coverage.
  -- A build is one consistent read, so every row at or below this block is present.
  select max(block_number) as max_block_number
  from {{ ref('tokens_arc_base_transfers') }}
  where {{ incremental_predicate('block_time') }}
),

arc_native_address as (
  select token_address
  from {{ source('dune', 'blockchains') }}
  where name = 'arc'
),

native_interface_movements as (
  select
    cast(date_trunc('day', i.evt_block_time) as date) as block_date,
    i.evt_tx_hash as tx_hash,
    cast(i.evt_index as bigint) as evt_index,
    i."from" as "from",
    i.to as to,
    i.value as amount_raw
  from {{ source('erc20_arc', 'evt_Transfer') }} i
  where i.contract_address = 0x3600000000000000000000000000000000000000
    and i.evt_block_number <= (select max_block_number from model_coverage)
    -- the two accepted losses carry no emitter log and so no native row; excluded here
    -- and asserted as losses in the sibling deduplication test
    and i.value > uint256 '0'
    and i."from" <> i.to
    and {{ incremental_predicate('i.evt_block_time') }}
),

native_rows as (
  select
    block_date,
    tx_hash,
    evt_index,
    "from",
    to,
    contract_address,
    amount_raw
  from {{ ref('tokens_arc_base_transfers') }}
  where token_standard = 'native'
    and {{ incremental_predicate('block_time') }}
),

misaddressed_native as (
  select
    block_date,
    tx_hash,
    evt_index,
    'native_row_off_canonical_address' as failure
  from native_rows
  where contract_address is distinct from (select token_address from arc_native_address)
),

erc20_on_native_address as (
  select
    block_date,
    tx_hash,
    evt_index,
    'erc20_row_on_native_address' as failure
  from {{ ref('tokens_arc_base_transfers') }}
  where token_standard <> 'native'
    and contract_address = (select token_address from arc_native_address)
    and {{ incremental_predicate('block_time') }}
),

-- Asked from the interface side on purpose. The reverse -- does every native row match some
-- interface log -- false-positives when one transaction carries both a paired movement and
-- a plain native send between the same two addresses, since the unpaired send then sees a
-- sibling log for its counterparties but none matching its own amount.
underscaled_native as (
  select
    i.block_date,
    i.tx_hash,
    i.evt_index,
    'native_amount_not_18_decimals' as failure
  from native_interface_movements i
  where not exists (
    select 1
    from native_rows n
    where n.tx_hash = i.tx_hash
      and n."from" = i."from"
      and n.to = i.to
      -- 6 decimals on the interface, 18 on the emitter
      and n.amount_raw = i.amount_raw * uint256 '1000000000000'
  )
)

select * from misaddressed_native
union all
select * from erc20_on_native_address
union all
select * from underscaled_native
