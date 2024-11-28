{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'covers_v2',
    materialized = 'view',
    unique_key = ['cover_id', 'staking_pool'],
    post_hook = '{{ expose_spells(blockchains = \'["ethereum"]\',
                                  spell_type = "project",
                                  spell_name = "nexusmutual",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with

cover_sales as (
  select
    c.call_block_time as block_time,
    c.call_block_number as block_number,
    c.output_coverId as cover_id,
    c.call_block_time as cover_start_time,
    date_add('second', cast(json_query(c.params, 'lax $.period') as bigint), c.call_block_time) as cover_end_time,
    cast(json_query(c.params, 'lax $.period') as bigint) as cover_period_seconds,
    cast(json_query(t.pool_allocation, 'lax $.poolId') as uint256) as pool_id,
    cast(json_query(c.params, 'lax $.productId') as uint256) as product_id,
    from_hex(json_query(c.params, 'lax $.owner' omit quotes)) as cover_owner,
    cast(json_query(c.params, 'lax $.amount') as uint256) as sum_assured,
    cast(json_query(c.params, 'lax $.coverAsset') as int) as cover_asset,
    cast(json_query(c.params, 'lax $.paymentAsset') as int) as payment_asset,
    cast(json_query(c.params, 'lax $.maxPremiumInAsset') as uint256) as max_premium_in_asset,
    cast(json_query(c.params, 'lax $.commissionRatio') as double) as commission_ratio,
    from_hex(json_query(c.params, 'lax $.commissionDestination' omit quotes)) as commission_destination,
    cast(json_query(t.pool_allocation, 'lax $.coverAmountInAsset') as uint256) as cover_amount_in_asset,
    cast(json_query(t.pool_allocation, 'lax $.skip') as boolean) as pool_allocation_skip,
    c.call_trace_address as trace_address,
    c.call_tx_hash as tx_hash
  from {{ source('nexusmutual_ethereum', 'Cover_call_buyCover') }} c
    cross join unnest(c.poolAllocationRequests) as t(pool_allocation)
  where c.call_success
),

staking_product_premiums as (
  select
    call_block_time as block_time,
    call_block_number as block_number,
    poolId as pool_id,
    productId as product_id,
    output_premium as premium,
    coverAmount as cover_amount,
    initialCapacityUsed as initial_capacity_used,
    totalCapacity as total_capacity,
    globalMinPrice as global_min_price,
    useFixedPrice as use_fixed_price,
    nxmPerAllocationUnit as nxm_per_allocation_unit,
    allocationUnitsPerNXM as allocation_units_per_nxm,
    cast(period as bigint) as premium_period_seconds,
    case
      when date_add('second', cast(period as bigint), call_block_time) > now()
      then (to_unixTime(now()) - to_unixTime(call_block_time)) / cast(period as bigint)
      else 1
    end as premium_period_ratio,
    call_trace_address,
    call_tx_hash as tx_hash
  from {{ source('nexusmutual_ethereum', 'StakingProducts_call_getPremium') }}
  where call_success
    and contract_address = 0xcafea573fbd815b5f59e8049e71e554bde3477e4
),

cover_premiums as (
  select
    c.block_time,
    c.block_number,
    c.cover_id,
    c.cover_start_time,
    c.cover_end_time,
    c.pool_id,
    c.product_id,
    p.cover_amount / 100.0 as partial_cover_amount, -- partial_cover_amount_in_nxm
    p.premium / 1e18 as premium,
    p.premium_period_ratio,
    c.commission_ratio / 10000.0 as commission_ratio,
    (c.commission_ratio / 10000.0) * p.premium / 1e18 as commission,
    (1.0 + (c.commission_ratio / 10000.0)) * p.premium / 1e18 as premium_incl_commission,
    case c.cover_asset
      when 0 then 'ETH'
      when 1 then 'DAI'
      when 6 then 'USDC'
      when 7 then 'cbBTC'
      else 'NA'
    end as cover_asset,
    c.sum_assured / case c.cover_asset when 6 then 1e6 when 7 then 1e8 else 1e18 end as sum_assured,
    case c.payment_asset
      when 0 then 'ETH'
      when 1 then 'DAI'
      when 6 then 'USDC'
      when 7 then 'cbBTC'
      when 255 then 'NXM'
      else 'NA'
    end as premium_asset,
    c.cover_owner,
    c.commission_destination,
    c.trace_address,
    c.tx_hash
  from cover_sales c
    inner join staking_product_premiums p on c.tx_hash = p.tx_hash and c.block_number = p.block_number
      and c.pool_id = p.pool_id and c.product_id = p.product_id
),

products as (
  select
    p.product_id,
    p.product_name,
    pt.product_type_id,
    pt.product_type_name as product_type
  from {{ ref('nexusmutual_ethereum_product_types_v2') }} pt
    inner join {{ ref('nexusmutual_ethereum_products_v2') }} p on pt.product_type_id = p.product_type_id
),

covers_v2 as (
  select
    cp.block_time,
    cp.block_number,
    cp.cover_id,
    cp.cover_start_time,
    cp.cover_end_time,
    cp.pool_id,
    cp.product_id,
    p.product_type,
    p.product_name,
    cp.partial_cover_amount,
    cp.cover_asset,
    cp.sum_assured,
    cp.premium_asset,
    cp.premium_period_ratio,
    cp.premium,
    cp.premium_incl_commission,
    cp.cover_owner,
    cp.commission,
    cp.commission_ratio,
    cp.commission_destination,
    cp.trace_address,
    cp.tx_hash
  from cover_premiums cp
    left join products p on cp.product_id = p.product_id
),

covers_v1_migrated as (
  select
    cm.evt_block_time as block_time,
    cm.evt_block_number as block_number,
    cm.coverIdV2 as cover_id,
    cv1.cover_start_time,
    cv1.cover_end_time,
    cv1.premium,
    cv1.premium_nxm,
    cv1.sum_assured,
    cv1.syndicate,
    cv1.product_name,
    cv1.product_type,
    cv1.cover_asset,
    cv1.premium_asset,
    cast(null as double) as premium_period_ratio,
    cm.newOwner as cover_owner,
    cast(null as double) as commission,
    cast(null as double) as commission_ratio,
    cast(null as varbinary) as commission_destination,
    cm.evt_index,
    cm.evt_tx_hash as tx_hash
  from {{ source('nexusmutual_ethereum', 'CoverMigrator_evt_CoverMigrated') }} cm
    inner join {{ ref('nexusmutual_ethereum_covers_v1') }} cv1 on cm.coverIdV1 = cv1.cover_id
),

covers as (
  select
    block_time,
    block_number,
    cover_id,
    cover_start_time,
    cover_end_time,
    pool_id as staking_pool_id,
    cast(pool_id as varchar) as staking_pool,
    cast(product_id as int) as product_id,
    product_type,
    product_name,
    cover_asset,
    premium_asset,
    premium,
    premium as premium_nxm,
    premium_incl_commission,
    premium_period_ratio,
    sum_assured,
    partial_cover_amount, -- in NXM
    cover_owner,
    commission,
    commission_ratio,
    commission_destination,
    false as is_migrated,
    trace_address,
    tx_hash
  from covers_v2
  union all
  select
    block_time,
    block_number,
    cover_id,
    cover_start_time,
    cover_end_time,
    cast(null as uint256) as staking_pool_id,
    syndicate as staking_pool,
    cast(null as int) as product_id,
    product_type,
    product_name,
    cover_asset,
    premium_asset,
    premium,
    premium_nxm,
    premium_nxm as premium_incl_commission,
    premium_period_ratio,
    sum_assured,
    sum_assured as partial_cover_amount, -- No partial covers in v1 migrated covers
    cover_owner,
    commission,
    commission_ratio,
    commission_destination,
    true as is_migrated,
    null as trace_address,
    tx_hash
  from covers_v1_migrated
)

select
  block_time,
  date_trunc('day', block_time) as block_date,
  block_number,
  cover_id,
  cover_start_time,
  cover_end_time,
  date_trunc('day', cover_start_time) as cover_start_date,
  date_trunc('day', cover_end_time) as cover_end_date,
  staking_pool_id,
  staking_pool,
  product_id,
  product_type,
  product_name,
  cover_asset,
  sum_assured,
  partial_cover_amount, -- in NXM
  premium_asset,
  premium,
  premium_nxm,
  premium_incl_commission,
  premium_period_ratio,
  cover_owner,
  commission,
  commission_ratio,
  commission_destination,
  is_migrated,
  trace_address,
  tx_hash
from covers
