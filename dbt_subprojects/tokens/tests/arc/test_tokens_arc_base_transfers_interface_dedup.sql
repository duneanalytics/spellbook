-- Native-interface deduplication for tokens_arc_base_transfers.
--
-- Purpose:
-- Arc's 6-decimal ERC-20 interface for USDC (0x3600...0000) is excluded from the erc20 leg
-- because the EIP-7708 emitter already carries the same movements at 18 decimals. Verify
-- (a) no interface log re-enters as an erc20 row, and (b) the only interface logs the
-- emitter does not mirror are the two documented accepted losses -- zero-value transfers
-- and self-transfers -- both balance-neutral. (b) is the guard that matters over time: if
-- the emitter ever stops mirroring the interface, excluding it would start losing real
-- movements, and this fails instead of silently under-reporting.
--
-- Native rows are deliberately not asserted away from that address: the model derives the
-- native address from dune.blockchains, so which address they carry follows that metadata.
-- Reconciliation of native rows against the emitter lives in the sibling test.
--
-- Failure interpretation:
-- interface_row_in_erc20_leg -- the exclusion broke and paired movements are double counted.
-- interface_log_lost_without_emitter_sibling -- a real movement is being dropped.

with interface_rows_in_erc20_leg as (
  select
    block_date,
    tx_hash,
    evt_index,
    'interface_row_in_erc20_leg' as failure
  from {{ ref('tokens_arc_base_transfers') }}
  where contract_address = 0x3600000000000000000000000000000000000000
    and token_standard <> 'native'
    and {{ incremental_predicate('block_time') }}
),

unmirrored_interface_logs as (
  select
    cast(date_trunc('day', i.evt_block_time) as date) as block_date,
    i.evt_tx_hash as tx_hash,
    cast(i.evt_index as bigint) as evt_index,
    'interface_log_lost_without_emitter_sibling' as failure
  from {{ source('erc20_arc', 'evt_Transfer') }} i
  where i.contract_address = 0x3600000000000000000000000000000000000000
    and {{ incremental_predicate('i.evt_block_time') }}
    -- accepted losses: the emitter requires from <> to, and a zero-value interface
    -- transfer emits no system log. Both move no balance.
    and i.value > uint256 '0'
    and i."from" <> i.to
    and not exists (
      select 1
      from {{ source('erc20_arc', 'evt_Transfer') }} e
      where e.contract_address = 0xfffffffffffffffffffffffffffffffffffffffe
        and {{ incremental_predicate('e.evt_block_time') }}
        and e.evt_tx_hash = i.evt_tx_hash
        and e."from" = i."from"
        and e.to = i.to
        -- 6 decimals on the interface, 18 on the emitter
        and e.value = i.value * uint256 '1000000000000'
    )
)

select * from interface_rows_in_erc20_leg
union all
select * from unmirrored_interface_logs
