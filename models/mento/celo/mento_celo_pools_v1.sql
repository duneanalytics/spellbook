{{
  config(
    tags = ['dunesql', 'static'],
    schema = 'mento_celo',
    alias = alias('pools_v1'),
    materialized = 'table'
  )
}}

select
  'celo' as blockchain,
  'mento' as project,
  'v1' as version,
  e.contract_address,
  concat(erc20a.symbol, '/', erc20b.symbol) as token_pair,
  e.asset0,
  erc20b.contract_address as asset1,
  from_hex('0x') as exchange_id, -- 0x instead of null so can be included in unique_key
  1 as is_active,
  e.block_time_created,
  e.block_number_created,
  cast(null as timestamp) as block_time_destroyed,
  cast(null as bigint) as block_number_destroyed
from (
    select contract_address, stable as asset0, evt_block_time as block_time_created, evt_block_number as block_number_created
    from {{ source('mento_celo', 'Exchange_evt_StableTokenSet') }}
    union all
    select contract_address, stable as asset0, evt_block_time as block_time_created, evt_block_number as block_number_created
    from {{ source('mento_celo', 'ExchangeEUR_evt_StableTokenSet') }}
    union all
    select contract_address, stable as asset0, evt_block_time as block_time_created, evt_block_number as block_number_created
    from {{ source('mento_celo', 'ExchangeBRL_evt_StableTokenSet') }}
  ) e
  left join {{ ref('tokens_erc20') }} erc20a on e.asset0 = erc20a.contract_address and erc20a.blockchain = 'celo'
  cross join {{ ref('tokens_erc20') }} erc20b
where erc20b.symbol = 'CELO'
  and erc20b.blockchain = 'celo'
