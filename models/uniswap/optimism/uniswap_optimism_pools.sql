 {{
  config(
        
        schema='uniswap_v3_optimism',
        alias='pools',
        materialized='table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "uniswap_v3",
                                    \'["msilb7", "chuxin", "hildobby"]\') }}'
  )
}}
with uniswap_v3_poolcreated as (
  select 
    'optimism' AS blockchain
    , 'uniswap' AS project
    , 'v3' AS version
    , pool
    , fee
    , array_agg(
        CAST(ROW(token0, token1) AS ROW(token0 VARBINARY, token1 VARBINARY))
    ) AS tokens
    , 2 AS tokens_in_pool
    , evt_block_time AS creation_block_time
    , evt_block_number AS creation_block_number
    , contract_address
  from {{ source('uniswap_v3_optimism', 'Factory_evt_PoolCreated') }}
)

select 
  'optimism' AS blockchain
  , 'uniswap' AS project
  , 'v3' AS version
  , newAddress as pool
  , fee
  , array_agg(
      CAST(ROW(token0, token1) AS ROW(token0 VARBINARY, token1 VARBINARY))
  ) AS tokens
  , 2 AS tokens_in_pool
  , creation_block_time
  , creation_block_number
  , contract_address
from {{ ref('uniswap_optimism_ovm1_pool_mapping') }}

union

select
  blockchain
  , project
  , version
  , pool
  , fee
  , array_agg(
      CAST(ROW(token0, token1) AS ROW(token0 VARBINARY, token1 VARBINARY))
  ) AS tokens
  , 2 AS tokens_in_pool
  , creation_block_time
  , creation_block_number
  , contract_address
from uniswap_v3_poolcreated
