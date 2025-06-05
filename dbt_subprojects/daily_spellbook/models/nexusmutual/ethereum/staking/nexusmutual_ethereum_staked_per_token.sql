{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'staked_per_token',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["ethereum"]\',
                                  spell_type = "project",
                                  spell_name = "nexusmutual",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with staked_per_pool_n_token as (
  select
    block_date,
    pool_id,
    pool_address,
    token_id,
    total_staked_nxm,
    stake_expiry_date,
    row_number() over (partition by pool_id, token_id order by block_date desc) as token_date_rn
  from {{ ref('nexusmutual_ethereum_base_staked_per_token') }}
)

select
  block_date,
  pool_id,
  pool_address,
  token_id,
  total_staked_nxm,
  stake_expiry_date,
  token_date_rn
from staked_per_pool_n_token
