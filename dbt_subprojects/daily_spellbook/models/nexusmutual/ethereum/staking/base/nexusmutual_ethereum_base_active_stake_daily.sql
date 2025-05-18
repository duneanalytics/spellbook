{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'base_active_stake_daily',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool_id', 'block_date'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook = '{{ expose_spells(blockchains = \'["ethereum"]\',
                                  spell_type = "project",
                                  spell_name = "nexusmutual",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with

staking_pools as (
  select
    pool_id,
    pool_address
  from {{ ref('nexusmutual_ethereum_staking_pools_list') }}
),

active_stake_updated as (
  select
    sp.pool_id,
    sp.pool_address,
    asu.evt_block_time as block_time,
    asu.evt_block_date as block_date,
    asu.evt_block_number as block_number,
    asu.activeStake / 1e18 as active_stake,
    asu.stakeSharesSupply as stake_shares_supply,
    asu.evt_tx_hash as tx_hash
  from {{ source('nexusmutual_ethereum', 'StakingPool_evt_ActiveStakeUpdated') }} asu
    inner join staking_pools sp on asu.contract_address = sp.pool_address
  {% if is_incremental() %}
  where {{ incremental_predicate('block_time') }}
  {% endif %}
),

active_stake_updated_daily as (
  select
    pool_id,
    pool_address,
    block_date,
    max_by(active_stake, block_date) as active_stake,
    max_by(stake_shares_supply, block_date) as stake_shares_supply,
    max_by(tx_hash, block_date) as tx_hash
  from active_stake_updated
  group by 1, 2, 3
)

select
  pool_id,
  pool_address,
  block_date,
  active_stake,
  stake_shares_supply,
  tx_hash
from active_stake_updated_daily
