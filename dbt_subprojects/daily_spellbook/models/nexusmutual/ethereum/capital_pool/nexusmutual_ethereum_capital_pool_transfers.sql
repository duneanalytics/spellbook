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

nexusmutual_contracts (protocol_contract_address, protocol_contract_type) as (
  values
  (0xcafeaf6eA90CB931ae43a8Cf4B25a73a24cF6158, 'capital pool'), --Pool (active), deployed: Oct-03-2024
  (0xcafeaBED7e0653aFe9674A3ad862b78DB3F36e60, 'capital pool'), --Pool, deployed: Nov-21-2023
  (0xcafea112Db32436c2390F5EC988f3aDB96870627, 'capital pool'), --Pool (Pool V2), deployed: Mar-08-2023
  (0xcafea35ce5a2fc4ced4464da4349f81a122fd12b, 'capital pool'), --Pool (Pool3), deployed: May-25-2021
  (0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8, 'capital pool'), --Pool (old), deployed: Jan-26-2021
  (0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb, 'capital pool'), --Pool2 (Pool 4), deployed: Jan-26-2021
  (0xfd61352232157815cf7b71045557192bf0ce1884, 'capital pool'), --Pool1, deployed: May-23-2019
  (0x7cbe5682be6b648cc1100c76d4f6c96997f753d6, 'capital pool'), --Pool2, deployed: May-23-2019
  (0xcafea8321b5109d22c53ac019d7a449c947701fb, 'mcr'), --MCR, deployed: May-25-2021
  (0xcafea92739e411a4D95bbc2275CA61dE6993C9a7, 'mcr'), --MCR, deployed: Nov-21-2023
  (0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e, 'ab multisig'), --Advisory Board multisig
  (0xcafeaed98d7Fce8F355C03c9F3507B90a974f37e, 'swap operator'), --SwapOperator, deployed: Oct-03-2024
  (0xcafea3ca5366964a102388ead5f3ebb0769c46cb, 'swap operator'), --SwapOperator, deployed: May-21-2024
  (0xcafea5c050e74a21c11af78c927e17853153097d, 'swap operator')  --SwapOperator, deployed: Jun-19-2023
),

transfer_in as (
  select
    t.block_time,
    t.block_number,
    date_trunc('day', t.block_time) as block_date,
    'in' as transfer_type,
    t."to" as to_address,
    t."from" as from_address,
    c_to.protocol_contract_type as to_address_type,
    coalesce(c_from.protocol_contract_type, 'external') as from_address_type,
    if(t.symbol = 'WETH', 'ETH', t.symbol) as symbol,
    t.amount,
    t.contract_address as token_contract_address,
    t.unique_key,
    t.tx_hash
  from {{ source('tokens_ethereum','transfers') }} t
    inner join nexusmutual_contracts c_to on t."to" = c_to.protocol_contract_address
    left join nexusmutual_contracts c_from on t."from" = c_from.protocol_contract_address
  where t.block_time >= timestamp '2019-05-01'
    and t.symbol in ('ETH', 'WETH', 'DAI', 'stETH', 'rETH', 'USDC', 'cbBTC')
    {% if is_incremental() %}
    and {{ incremental_predicate('t.block_time') }}
    {% endif %}
),

transfer_out as (
  select
    t.block_time,
    t.block_number,
    date_trunc('day', t.block_time) as block_date,
    'out' as transfer_type,
    t."to" as to_address,
    t."from" as from_address,
    coalesce(c_to.protocol_contract_type, 'external') as to_address_type,
    c_from.protocol_contract_type as from_address_type,
    if(t.symbol = 'WETH', 'ETH', t.symbol) as symbol,
    -1 * t.amount as amount,
    t.contract_address as token_contract_address,
    t.unique_key,
    t.tx_hash
  from {{ source('tokens_ethereum','transfers') }} t
    inner join nexusmutual_contracts c_from on t."from" = c_from.protocol_contract_address
    left join nexusmutual_contracts c_to on t."to" = c_to.protocol_contract_address
  where t.block_time >= timestamp '2019-05-01'
    and t.symbol in ('ETH', 'WETH', 'DAI', 'stETH', 'rETH', 'USDC', 'cbBTC')
    {% if is_incremental() %}
    and {{ incremental_predicate('t.block_time') }}
    {% endif %}
),

transfer_nxmty_in as (
  select
    t.block_time,
    t.block_number,
    date_trunc('day', t.block_time) as block_date,
    'in' as transfer_type,
    t."to" as to_address,
    t."from" as from_address,
    c_to.protocol_contract_type as to_address_type,
    coalesce(c_from.protocol_contract_type, 'external') as from_address_type,
    'NXMTY' as symbol,
    cast(t.amount_raw as double) / 1e18 as amount,
    t.contract_address as token_contract_address,
    t.unique_key,
    t.tx_hash
  from {{ source('tokens_ethereum','transfers') }} t
    inner join nexusmutual_contracts c_to on t."to" = c_to.protocol_contract_address
    left join nexusmutual_contracts c_from on t."from" = c_from.protocol_contract_address
  where t.block_time >= timestamp '2022-05-27'
    and t.contract_address = 0x27f23c710dd3d878fe9393d93465fed1302f2ebd --NXMTY
    {% if is_incremental() %}
    and {{ incremental_predicate('t.block_time') }}
    {% endif %}
),

transfer_nxmty_out as (
  select
    t.block_time,
    t.block_number,
    date_trunc('day', t.block_time) as block_date,
    'out' as transfer_type,
    t."to" as to_address,
    t."from" as from_address,
    coalesce(c_to.protocol_contract_type, 'external') as to_address_type,
    c_from.protocol_contract_type as from_address_type,
    'NXMTY' as symbol,
    -1 * cast(t.amount_raw as double) / 1e18 as amount,
    t.contract_address as token_contract_address,
    t.unique_key,
    t.tx_hash
  from {{ source('tokens_ethereum','transfers') }} t
    inner join nexusmutual_contracts c_from on t."from" = c_from.protocol_contract_address
    left join nexusmutual_contracts c_to on t."to" = c_to.protocol_contract_address
  where t.block_time >= timestamp '2022-05-27'
    and t.contract_address = 0x27f23c710dd3d878fe9393d93465fed1302f2ebd --NXMTY
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %}
)

select
  block_time, block_number, block_date, transfer_type, from_address, to_address, from_address_type, to_address_type, symbol, amount, token_contract_address, unique_key, tx_hash
from transfer_in
union all
select
  block_time, block_number, block_date, transfer_type, from_address, to_address, from_address_type, to_address_type, symbol, amount, token_contract_address, unique_key, tx_hash
from transfer_out
union all
select
  block_time, block_number, block_date, transfer_type, from_address, to_address, from_address_type, to_address_type, symbol, amount, token_contract_address, unique_key, tx_hash
from transfer_nxmty_in
union all
select
  block_time, block_number, block_date, transfer_type, from_address, to_address, from_address_type, to_address_type, symbol, amount, token_contract_address, unique_key, tx_hash
from transfer_nxmty_out
