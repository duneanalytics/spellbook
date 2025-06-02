{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'staking_pools_list',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool_id', 'pool_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook = '{{ expose_spells(blockchains = \'["ethereum"]\',
                                  spell_type = "project",
                                  spell_name = "nexusmutual",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

select
  evt_block_time as block_time,
  cast(poolId as int) as pool_id,
  stakingPoolAddress as pool_address,
  evt_tx_hash as tx_hash
from {{ source('nexusmutual_ethereum', 'StakingPoolFactory_evt_StakingPoolCreated') }}
{% if is_incremental() %}
where {{ incremental_predicate('evt_block_time') }}
{% endif %}
