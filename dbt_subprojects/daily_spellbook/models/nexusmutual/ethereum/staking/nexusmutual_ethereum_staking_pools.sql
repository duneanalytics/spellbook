{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'staking_pools',
    materialized = 'view',
    unique_key = ['pool_id', 'product_id'],
    post_hook = '{{ expose_spells(blockchains = \'["ethereum"]\',
                                  spell_type = "project",
                                  spell_name = "nexusmutual",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with staking_pools as (
  select *, row_number() over (partition by pool_id, product_id order by block_time_updated desc) as rn
  from {{ ref('nexusmutual_ethereum_base_staking_pools') }}
)

select
  block_time_created,
  block_time_updated,
  pool_id,
  pool_address,
  manager_address,
  manager_ens,
  manager,
  is_private_pool,
  initial_pool_fee,
  current_management_fee,
  max_management_fee,
  product_id,
  product_name,
  product_type,
  initial_price,
  target_price,
  initial_weight,
  target_weight,
  pool_created_time,
  product_added_time,
  tx_hash_created,
  tx_hash_updated
from staking_pools
where rn = 1
