-- No phantom ERC4626 mint/burn rows in tokens_ethereum_base_transfers.
--
-- Purpose:
-- transfers_base_erc4626 synthesizes mint/burn rows from Deposit/Withdraw only for share
-- tokens that never emit ERC20 mint/burn Transfer events (e.g. sDAI). A synthetic row
-- carries the Deposit/Withdraw (tx_hash, evt_index, contract_address) and has no
-- evt_Transfer row at that position. If its contract also emits real mint/burn Transfers,
-- the row double counts supply (e.g. async vaults that burn at settlement and emit
-- Withdraw at claim).
--
-- Failure interpretation:
-- phantom_erc4626_row -- a synthetic row was built for a contract that self-mints.

with erc4626_events as (
  select
    evt_tx_hash as tx_hash,
    evt_index,
    contract_address
  from {{ source('erc4626_ethereum', 'evt_deposit') }}
  where {{ incremental_predicate('evt_block_time') }}

  union all

  select
    evt_tx_hash as tx_hash,
    evt_index,
    contract_address
  from {{ source('erc4626_ethereum', 'evt_withdraw') }}
  where {{ incremental_predicate('evt_block_time') }}
),

self_minting_contracts as (
  select distinct
    contract_address
  from {{ source('erc20_ethereum', 'evt_Transfer') }}
  where ("from" = 0x0000000000000000000000000000000000000000
      or "to" = 0x0000000000000000000000000000000000000000)
    and evt_block_time >= now() - interval '365' day
    and contract_address in (select contract_address from erc4626_events)
),

erc20_transfers as (
  select
    evt_tx_hash as tx_hash,
    evt_index,
    contract_address
  from {{ source('erc20_ethereum', 'evt_Transfer') }}
  where {{ incremental_predicate('evt_block_time') }}
    and contract_address in (select contract_address from self_minting_contracts)
)

select
  t.block_date,
  t.tx_hash,
  t.evt_index,
  t.contract_address,
  'phantom_erc4626_row' as failure
from {{ ref('tokens_ethereum_base_transfers') }} t
inner join erc4626_events e
  on e.tx_hash = t.tx_hash
  and e.evt_index = t.evt_index
  and e.contract_address = t.contract_address
inner join self_minting_contracts smc
  on smc.contract_address = t.contract_address
left join erc20_transfers r
  on r.tx_hash = t.tx_hash
  and r.evt_index = t.evt_index
  and r.contract_address = t.contract_address
where r.tx_hash is null
  and {{ incremental_predicate('t.block_time') }}
