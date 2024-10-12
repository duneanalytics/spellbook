{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'capital_pool_transfers',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'transfer_type', 'symbol', 'unique_key', 'tx_hash'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook = '{{ expose_spells(blockchains = \'["ethereum"]\',
                                  spell_type = "project",
                                  spell_name = "nexusmutual",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with

nexusmutual_contracts (contract_address) as (
  values
  (0xcafeaf6eA90CB931ae43a8Cf4B25a73a24cF6158), --Pool (active), deployed: Oct-03-2024
  (0xcafeaBED7e0653aFe9674A3ad862b78DB3F36e60), --Pool, deployed: Nov-21-2023
  (0xcafea112Db32436c2390F5EC988f3aDB96870627), --Pool (Pool V2), deployed: Mar-08-2023
  (0xcafea35ce5a2fc4ced4464da4349f81a122fd12b), --Pool (Pool3), deployed: May-25-2021
  (0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8), --Pool (old), deployed: Jan-26-2021
  (0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb), --Pool2 (Pool 4), deployed: Jan-26-2021
  (0xfd61352232157815cf7b71045557192bf0ce1884), --Pool1, deployed: May-23-2019
  (0x7cbe5682be6b648cc1100c76d4f6c96997f753d6), --Pool2, deployed: May-23-2019
  (0xcafea8321b5109d22c53ac019d7a449c947701fb), --MCR, deployed: May-25-2021
  (0xcafea92739e411a4D95bbc2275CA61dE6993C9a7), --MCR, deployed: Nov-21-2023
  (0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e)  --Advisory Board multisig
),

transfer_in as (
  select
    block_time,
    block_number,
    date_trunc('day', block_time) as block_date,
    'in' as transfer_type,
    symbol,
    amount,
    contract_address,
    unique_key,
    tx_hash
  from {{ source('tokens_ethereum','transfers') }}
  where block_time >= timestamp '2019-05-23'
    and "to" in (select contract_address from nexusmutual_contracts)
    and symbol in ('ETH', 'DAI', 'stETH', 'rETH', 'USDC')
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %}
),

transfer_out as (
  select
    block_time,
    block_number,
    date_trunc('day', block_time) as block_date,
    'out' as transfer_type,
    symbol,
    -1 * amount as amount,
    contract_address,
    unique_key,
    tx_hash
  from {{ source('tokens_ethereum','transfers') }}
  where block_time >= timestamp '2019-05-23'
    and "from" in (select contract_address from nexusmutual_contracts)
    and symbol in ('ETH', 'DAI', 'stETH', 'rETH', 'USDC')
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %}
),

transfer_nxmty_in as (
  select
    block_time,
    block_number,
    date_trunc('day', block_time) as block_date,
    'in' as transfer_type,
    'NXMTY' as symbol,
    cast(amount_raw as double) / 1e18 as amount,
    contract_address,
    unique_key,
    tx_hash
  from {{ source('tokens_ethereum','transfers') }}
  where block_time >= timestamp '2022-05-27'
    and "to" in (select contract_address from nexusmutual_contracts)
    and contract_address = 0x27f23c710dd3d878fe9393d93465fed1302f2ebd --NXMTY
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %}
),

transfer_nxmty_out as (
  select
    block_time,
    block_number,
    date_trunc('day', block_time) as block_date,
    'out' as transfer_type,
    'NXMTY' as symbol,
    -1 * cast(amount_raw as double) / 1e18 as amount,
    contract_address,
    unique_key,
    tx_hash
  from {{ source('tokens_ethereum','transfers') }}
  where block_time >= timestamp '2022-05-27'
    and "from" in (select contract_address from nexusmutual_contracts)
    and contract_address = 0x27f23c710dd3d878fe9393d93465fed1302f2ebd --NXMTY
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %}
)

select block_time, block_number, block_date, transfer_type, symbol, amount, contract_address, unique_key, tx_hash
from transfer_in
union all
select block_time, block_number, block_date, transfer_type, symbol, amount, contract_address, unique_key, tx_hash
from transfer_out
union all
select block_time, block_number, block_date, transfer_type, symbol, amount, contract_address, unique_key, tx_hash
from transfer_nxmty_in
union all
select block_time, block_number, block_date, transfer_type, symbol, amount, contract_address, unique_key, tx_hash
from transfer_nxmty_out
where 1=1 -- dummy change to trigger re-run
