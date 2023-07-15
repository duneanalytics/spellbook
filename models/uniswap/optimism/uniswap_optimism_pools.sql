 {{
  config(
        schema='uniswap_v3_optimism',
        alias = alias('pools'),
        materialized='table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "uniswap_v3",
                                    \'["msilb7", "chuxin"]\') }}'
  )
}}
with uniswap_v3_poolcreated as (
  select 
    pool
    ,token0
    ,token1
    ,fee
  from {{ source('uniswap_v3_optimism', 'factory_evt_poolcreated') }} 
  group by 1, 2, 3, 4
)

select 
   newAddress as pool
  , LOWER(token0) AS token0
  , LOWER(token1) AS token1
  ,fee
from {{ ref('uniswap_optimism_ovm1_pool_mapping') }}

union

select
  pool
  , LOWER(token0) AS token0
  , LOWER(token1) AS token1
  , fee
from uniswap_v3_poolcreated
