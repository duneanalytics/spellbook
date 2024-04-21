{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'staking_pools',
    materialized = 'view',
    unique_key = ['pool_id', 'product_id'],
    post_hook = '{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "nexusmutual",
                                \'["tomfutago"]\') }}'
  )
}}

with

staking_pool_names (pool_id, pool_name) as (
  values
  (1, 'Nexus Foundation'),
  (2, 'Hugh'),
  (3, 'Ease AAA Low Risk Pool'),
  (4, 'Ease AA Medium Risk Pool'),
  (5, 'Unity Cover'),
  (6, 'Safe Invest'),
  (7, 'ShieldX Staking Pool'),
  (8, 'DeFiSafety X OpenCover Blue Chip Protocol Pool'),
  (9, 'My Conservative Pool'),
  (10, 'SAFU Pool'),
  (11, 'Sherlock'),
  (12, 'Gm Exit Here (GLP) Pool'),
  (13, 'My Nexus Pool'),
  (14, 'My Private Pool'),
  (15, 'Reflection'),
  (16, 'Good KarMa Capital'),
  (17, 'High Trust Protocols'),
  (18, 'UnoRe WatchDog Pool'),
  (19, 'Broad And Diversified'),
  (20, 'Lowest Risk'),
  (21, 'Crypto Plaza'),
  (22, 'BraveNewDeFi''s Pool')
),

staking_pools_created as (
  select
    call_block_time as block_time,
    output_0 as pool_id,
    output_1 as pool_address,
    isPrivatePool as is_private_pool,
    initialPoolFee as initial_pool_fee,
    maxPoolFee as max_management_fee,
    productInitParams as params
  from {{ source('nexusmutual_ethereum', 'Cover_call_createStakingPool') }}
  where call_success
    and contract_address = 0xcafeac0fF5dA0A2777d915531bfA6B29d282Ee62
),

staking_pools_and_products as (
  select
    sp.block_time,
    sp.pool_id,
    sp.pool_address,
    sp.is_private_pool,
    sp.initial_pool_fee,
    sp.max_management_fee,
    cast(json_query(t.json, 'lax $.productId') as int) as product_id,
    cast(json_query(t.json, 'lax $.weight') as double) as weight,
    cast(json_query(t.json, 'lax $.initialPrice') as double) as initial_price,
    cast(json_query(t.json, 'lax $.targetPrice') as double) as target_price
  from staking_pools_created as sp
    left join unnest(params) as t(json) on true
),

staking_pool_products_updated as (
  select
    *,
    row_number() over (partition by pool_id, product_id order by block_time desc) as rn
  from (
    select
      call_block_time as block_time,
      poolId as pool_id,
      cast(json_query(t.json, 'lax $.productId') as int) as product_id,
      cast(json_query(t.json, 'lax $.recalculateEffectiveWeight') as boolean) as re_eval_eff_weight,
      cast(json_query(t.json, 'lax $.setTargetWeight') as boolean) as set_target_weight,
      cast(json_query(t.json, 'lax $.targetWeight') as double) as target_weight,
      cast(json_query(t.json, 'lax $.setTargetPrice') as boolean) as set_target_price,
      cast(json_query(t.json, 'lax $.targetPrice') as double) as target_price
    from {{ source('nexusmutual_ethereum', 'StakingProducts_call_setProducts') }} as p
      cross join unnest(params) as t(json)
    where call_success
      and contract_address = 0xcafea573fBd815B5f59e8049E71E554bde3477E4
      and cast(json_query(t.json, 'lax $.setTargetWeight') as boolean) = true
  ) t
),

staking_pool_products_combined as (
  select
    coalesce(spp.pool_id, spu.pool_id) as pool_id,
    coalesce(spp.product_id, spu.product_id) as product_id,
    spp.initial_price,
    spp.target_price,
    spu.target_price as updated_target_price,
    spp.weight as initial_weight,
    spu.target_weight,
    spu.block_time as updated_time,
    if(spp.product_id is null, true, false) as is_product_added
  from staking_pools_and_products spp
    full outer join staking_pool_products_updated spu on spp.pool_id = spu.pool_id and spp.product_id = spu.product_id
  where coalesce(spu.rn, 1) = 1
),

staking_pool_managers_history as (
  select
    call_block_time as block_time,
    poolId as pool_id,
    manager,
    call_trace_address,
    call_tx_hash as tx_hash
  from {{ source('nexusmutual_ethereum', 'TokenController_call_assignStakingPoolManager') }}
  where call_success
  union all
  select
    call_block_time as block_time,
    poolId as pool_id,
    manager,
    call_trace_address,
    call_tx_hash as tx_hash
  from {{ source('nexusmutual_ethereum', 'TokenController2_call_assignStakingPoolManager') }}
  where call_success
  union all
  select distinct
    m.call_block_time as block_time,
    sp.poolId as pool_id,
    m.output_0 as manager,
    m.call_trace_address,
    m.call_tx_hash as tx_hash
  from {{ source('nexusmutual_ethereum', 'StakingProducts_evt_ProductUpdated') }} pu
    inner join {{ source('nexusmutual_ethereum', 'StakingProducts_call_setProducts') }} sp on pu.evt_tx_hash = call_tx_hash and pu.evt_block_number = sp.call_block_number
    inner join {{ source('nexusmutual_ethereum', 'StakingPool_call_manager') }} m on sp.call_tx_hash = m.call_tx_hash and sp.call_block_number = m.call_block_number
  where sp.call_success
    and m.call_success
),

staking_pool_managers as (
  select
    t.pool_id,
    t.manager as manager_address,
    ens.name as manager_ens,
    coalesce(ens.name, cast(t.manager as varchar)) as manager
  from (
      select
        pool_id,
        manager,
        row_number() over (partition by pool_id order by block_time, call_trace_address desc) as rn
      from staking_pool_managers_history
    ) t
    left join labels.ens on t.manager = ens.address
  where t.rn = 1
),

staking_pool_fee_updates as (
  select
    sp.pool_id,
    t.pool_address,
    t.new_fee
  from (
      select
        evt_block_time as block_time,
        contract_address as pool_address,
        newFee as new_fee,
        evt_tx_hash as tx_hash,
        row_number() over (partition by contract_address order by evt_block_time desc, evt_index desc) as rn
      from {{ source('nexusmutual_ethereum', 'StakingPool_evt_PoolFeeChanged') }}
    ) t
    inner join staking_pools_created sp on t.pool_address = sp.pool_address
  where t.rn = 1
)

select
  sp.pool_id,
  sp.pool_address,
  spn.pool_name,
  spm.manager_address,
  spm.manager_ens,
  spm.manager,
  sp.is_private_pool,
  sp.initial_pool_fee,
  coalesce(spf.new_fee, sp.initial_pool_fee) as current_pool_fee,
  sp.max_management_fee,
  spc.product_id,
  spc.initial_price,
  coalesce(spc.updated_target_price, spc.target_price) as target_price,
  spc.initial_weight,
  spc.target_weight,
  sp.block_time as pool_created_time,
  if(spc.is_product_added, spc.updated_time, sp.block_time) as product_added_time
from staking_pools_created sp
  inner join staking_pool_products_combined spc on sp.pool_id = spc.pool_id
  left join staking_pool_names spn on sp.pool_id = spn.pool_id
  left join staking_pool_managers spm on sp.pool_id = spm.pool_id
  left join staking_pool_fee_updates spf on sp.pool_id = spf.pool_id
