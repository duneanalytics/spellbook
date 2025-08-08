 {{
  config(
      schema='uniswap_optimism',
      alias = 'pools',
      materialized = 'view'
  )
}}

select 
  blockchain
  , project
  , version
  , contract_address
  , creation_block_time
  , creation_block_number
  , pool as id 
  , fee
  , cast(null as varbinary) as tx_hash -- or use null as varbinary
  , 0 as evt_index 
  , token0
  , token1
from 
{{ ref('uniswap_v3_optimism_pools') }} -- V3 pools (including ovm1)

union all 

select 
  blockchain
  , project
  , version
  , contract_address
  , creation_block_time
  , creation_block_number
  , id
  , fee
  , tx_hash
  , evt_index 
  , token0
  , token1
from 
{{ ref('uniswap_v2_optimism_pools') }}

union all 

select 
  blockchain
  , project
  , version
  , contract_address
  , creation_block_time
  , creation_block_number
  , id
  , fee
  , tx_hash
  , evt_index 
  , token0
  , token1
from 
{{ ref('uniswap_v4_optimism_pools') }}
