{{
  config(
    schema = 'mento_v1_celo',
    alias = 'pools',
    tags = ['static'],
    materialized = 'table'
  )
}}

select
  'celo' as blockchain,
  'mento' as project,
  '1' as version,
  e.contract_address as pool,
  cast(null as decimal(38,1)) as fee,
  e.asset0 as token0,
  0x471EcE3750Da237f93B8E339c536989b8978a438 as token1,
  e.evt_block_time as creation_block_time,
  e.evt_block_number as creation_block_number,
  e.contract_address
from (
    select contract_address, stable as asset0, evt_block_time, evt_block_number
    from {{ source('mento_celo', 'Exchange_evt_StableTokenSet') }}
    union all
    select contract_address, stable as asset0, evt_block_time, evt_block_number
    from {{ source('mento_celo', 'ExchangeEUR_evt_StableTokenSet') }}
    union all
    select contract_address, stable as asset0, evt_block_time, evt_block_number
    from {{ source('mento_celo', 'ExchangeBRL_evt_StableTokenSet') }}
  ) e
