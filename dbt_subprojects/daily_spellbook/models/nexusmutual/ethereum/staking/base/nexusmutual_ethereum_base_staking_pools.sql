{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'base_staking_pools',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool_id', 'product_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time_updated')]
  )
}}

with

staking_pools_created as (
  select
    call_block_time as block_time_created,
    output_0 as pool_id,
    output_1 as pool_address,
    isPrivatePool as is_private_pool,
    initialPoolFee as initial_pool_fee,
    maxPoolFee as max_management_fee,
    productInitParams as params,
    call_tx_hash as tx_hash_created
  from {{ source('nexusmutual_ethereum', 'Cover_call_createStakingPool') }}
  where call_success
    and contract_address = 0xcafeac0fF5dA0A2777d915531bfA6B29d282Ee62 -- proxy
  union all
  select
    call_block_time as block_time_created,
    output_0 as pool_id,
    output_1 as pool_address,
    isPrivatePool as is_private_pool,
    initialPoolFee as initial_pool_fee,
    maxPoolFee as max_management_fee,
    productInitParams as params,
    call_tx_hash as tx_hash_created
  from {{ source('nexusmutual_ethereum', 'StakingProducts_call_createStakingPool') }}
  where call_success
),

staking_pools_created_ext as (
  select
    spe.block_time as block_time_created,
    spe.pool_id,
    spe.pool_address,
    spc.is_private_pool,
    spc.initial_pool_fee,
    spc.max_management_fee,
    spc.params,
    spe.tx_hash as tx_hash_created
  from {{ ref('nexusmutual_ethereum_staking_pools_list') }} spe
    inner join staking_pools_created spc on spe.pool_id = spc.pool_id and spe.block_time = spc.block_time_created
),

staking_pools_and_products as (
  select
    sp.block_time_created,
    sp.pool_id,
    sp.pool_address,
    sp.is_private_pool,
    sp.initial_pool_fee,
    sp.max_management_fee,
    coalesce(cast(json_query(t.json, 'lax $.productId') as int), -1) as product_id,
    cast(json_query(t.json, 'lax $.weight') as double) as weight,
    cast(json_query(t.json, 'lax $.initialPrice') as double) as initial_price,
    cast(json_query(t.json, 'lax $.targetPrice') as double) as target_price,
    sp.tx_hash_created
  from staking_pools_created_ext as sp
    left join unnest(sp.params) as t(json) on true
),

staking_pool_products_updated as (
  select
    *,
    row_number() over (partition by pool_id, product_id order by block_time_updated desc) as rn
  from (
    select
      p.call_block_time as block_time_updated,
      p.poolId as pool_id,
      coalesce(cast(json_query(t.json, 'lax $.productId') as int), -1) as product_id,
      cast(json_query(t.json, 'lax $.recalculateEffectiveWeight') as boolean) as re_eval_eff_weight,
      cast(json_query(t.json, 'lax $.setTargetWeight') as boolean) as set_target_weight,
      cast(json_query(t.json, 'lax $.targetWeight') as double) as target_weight,
      cast(json_query(t.json, 'lax $.setTargetPrice') as boolean) as set_target_price,
      cast(json_query(t.json, 'lax $.targetPrice') as double) as target_price,
      p.call_tx_hash as tx_hash_updated
    from {{ source('nexusmutual_ethereum', 'StakingProducts_call_setProducts') }} as p
      cross join unnest(params) as t(json)
    where p.call_success
      --and p.contract_address = 0xcafea573fBd815B5f59e8049E71E554bde3477E4
      and p.contract_address <> 0xcafea524e89514e131ee9f8462536793d49d8738
      and cast(json_query(t.json, 'lax $.setTargetWeight') as boolean) = true
  ) t
),

staking_pool_products_combined as (
  select
    spp.block_time_created,
    spu.block_time_updated,
    spp.pool_id,
    coalesce(spp.product_id, -1) as product_id,
    spp.initial_price,
    coalesce(spu.target_price, spp.target_price) as target_price,
    spp.weight as initial_weight,
    spu.target_weight,
    if(spp.product_id is null, true, false) as is_product_added,
    spp.tx_hash_created,
    spu.tx_hash_updated
  from staking_pools_and_products spp
    left join staking_pool_products_updated spu
      on spp.pool_id = spu.pool_id
      and spp.product_id = spu.product_id
      and spu.rn = 1
  
  union all
  
  select
    spu.block_time_updated as block_time_created,
    spu.block_time_updated,
    spu.pool_id,
    coalesce(spu.product_id, -1) as product_id,
    null as initial_price,
    spu.target_price as target_price,
    null as initial_weight,
    spu.target_weight,
    true as is_product_added,
    null as tx_hash_created,
    spu.tx_hash_updated
  from staking_pool_products_updated spu
    left join staking_pools_and_products spp
      on spu.pool_id = spp.pool_id
      and spu.product_id = spp.product_id
  where spp.pool_id is null
    and spu.rn = 1
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
    and contract_address = 0x5407381b6c251cfd498ccd4a1d877739cb7960b8 -- proxy
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
    t.block_time as block_time_updated,
    t.pool_id,
    t.manager as manager_address,
    ens.name as manager_ens,
    coalesce(ens.name, cast(t.manager as varchar)) as manager,
    t.tx_hash as tx_hash_updated
  from (
      select
        block_time,
        pool_id,
        manager,
        tx_hash,
        row_number() over (partition by pool_id order by block_time, call_trace_address desc) as rn
      from staking_pool_managers_history
    ) t
    left join labels.ens on t.manager = ens.address
  where t.rn = 1
),

staking_pool_fee_updates as (
  select
    block_time as block_time_updated,
    pool_address,
    new_fee,
    tx_hash as tx_hash_updated
  from (
      select
        evt_block_time as block_time,
        contract_address as pool_address,
        newFee as new_fee,
        evt_tx_hash as tx_hash,
        row_number() over (partition by contract_address order by evt_block_time desc, evt_index desc) as rn
      from {{ source('nexusmutual_ethereum', 'StakingPool_evt_PoolFeeChanged') }}
    ) t
  where t.rn = 1
),

products as (
  select
    cast(p.product_id as int) as product_id,
    p.product_name,
    cast(pt.product_type_id as int) as product_type_id,
    pt.product_type_name as product_type
  from {{ ref('nexusmutual_ethereum_product_types_v2') }} pt
    inner join {{ ref('nexusmutual_ethereum_products_v2') }} p on pt.product_type_id = p.product_type_id
  union all
  select
    -1 as product_id,
    null as product_name,
    -1 as product_type_id,
    null as product_type
),

staking_pools_final as (
  select
    sp.block_time_created,
    spc.block_time_updated as block_time_product_updated,
    spm.block_time_updated as block_time_manager_updated,
    spf.block_time_updated as block_time_fee_updated,
    greatest(
      coalesce(spc.block_time_updated, sp.block_time_created),
      coalesce(spm.block_time_updated, sp.block_time_created),
      coalesce(spf.block_time_updated, sp.block_time_created)
    ) as block_time_updated,
    sp.pool_id,
    sp.pool_address,
    spm.manager_address,
    spm.manager_ens,
    spm.manager,
    sp.is_private_pool,
    sp.initial_pool_fee / 100.00 as initial_pool_fee,
    coalesce(spf.new_fee, sp.initial_pool_fee) / 100.00 as current_management_fee,
    sp.max_management_fee / 100.00 as max_management_fee,
    spc.product_id,
    p.product_name,
    p.product_type,
    spc.initial_price / 100.00 as initial_price,
    spc.target_price / 100.00 as target_price,
    spc.initial_weight / 100.00 as initial_weight,
    spc.target_weight / 100.00 as target_weight,
    sp.block_time_created as pool_created_time,
    if(spc.is_product_added, spc.block_time_updated, sp.block_time_created) as product_added_time,
    sp.tx_hash_created,
    spc.tx_hash_updated as tx_hash_product_updated,
    spm.tx_hash_updated as tx_hash_manager_updated,
    spf.tx_hash_updated as tx_hash_fee_updated,
    greatest(
      coalesce(spc.tx_hash_updated, sp.tx_hash_created),
      coalesce(spm.tx_hash_updated, sp.tx_hash_created),
      coalesce(spf.tx_hash_updated, sp.tx_hash_created)
    ) as tx_hash_updated
  from staking_pools_created_ext sp
    inner join staking_pool_products_combined spc on sp.pool_id = spc.pool_id
    inner join products p on spc.product_id = p.product_id
    left join staking_pool_managers spm on sp.pool_id = spm.pool_id
    left join staking_pool_fee_updates spf on sp.pool_address = spf.pool_address
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
from staking_pools_final
{% if is_incremental() %}
where {{ incremental_predicate('block_time_updated') }}
{% endif %}
