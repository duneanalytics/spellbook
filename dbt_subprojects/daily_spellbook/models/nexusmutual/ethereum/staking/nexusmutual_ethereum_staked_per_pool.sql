{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'staked_per_pool',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["ethereum"]\',
                                  spell_type = "project",
                                  spell_name = "nexusmutual",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with staked_per_pool as (
  select
    block_date,
    pool_id,
    pool_address,
    total_staked_nxm,
    row_number() over (partition by pool_id order by block_date desc) as pool_date_rn
  from {{ ref('nexusmutual_ethereum_base_staked_per_pool') }}
)

select
  block_date,
  pool_id,
  pool_address,
  total_staked_nxm,
  pool_date_rn
from staked_per_pool
