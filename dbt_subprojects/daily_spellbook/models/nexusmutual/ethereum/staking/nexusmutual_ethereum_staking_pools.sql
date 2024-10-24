{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'staking_pools',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool_id', 'product_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time_updated')],
    post_hook = '{{ expose_spells(blockchains = \'["ethereum"]\',
                                  spell_type = "project",
                                  spell_name = "nexusmutual",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}
-- trigger CI

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
from {{ ref('nexusmutual_ethereum_base_staking_pools') }}
{% if is_incremental() %}
where {{ incremental_predicate('block_time_updated') }}
{% endif %}
