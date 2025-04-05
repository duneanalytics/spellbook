{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'product_types_v2',
    materialized = 'view',
    unique_key = ['product_type_id'],
    post_hook = '{{ expose_spells(blockchains = \'["ethereum"]\',
                                  spell_type = "project",
                                  spell_name = "nexusmutual",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with

product_type_events as (
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    id as product_type_id,
    ipfsMetadata as evt_product_type_ipfs_metadata,
    evt_index,
    evt_tx_hash as tx_hash,
    row_number() over (partition by evt_block_time, evt_tx_hash order by evt_index) as evt_rn
  from {{ source('nexusmutual_ethereum', 'Cover_evt_ProductTypeSet') }}
  union all
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    id as product_type_id,
    cast(null as varchar) as evt_product_type_ipfs_metadata,
    evt_index,
    evt_tx_hash as tx_hash,
    row_number() over (partition by evt_block_time, evt_tx_hash order by evt_index) as evt_rn
  from {{ source('nexusmutual_ethereum', 'CoverProducts_evt_ProductTypeSet') }}
),

product_type_calls as (
  select
    call_block_time as block_time,
    call_block_number as block_number,
    productTypeParams,
    call_tx_hash as tx_hash,
    row_number() over (partition by call_block_time, call_tx_hash order by call_trace_address desc) as tx_call_rn
  from {{ source('nexusmutual_ethereum', 'Cover_call_setProductTypes') }}
  where call_success
  union all
  select
    call_block_time as block_time,
    call_block_number as block_number,
    productTypeParams,
    call_tx_hash as tx_hash,
    row_number() over (partition by call_block_time, call_tx_hash order by call_trace_address desc) as tx_call_rn
  from {{ source('nexusmutual_ethereum', 'CoverProducts_call_setProductTypes') }}
  where call_success
),

product_type_data_raw as (
  select
    pt.block_time,
    pt.block_number,
    pt.productTypeParams,
    cast(json_extract_scalar(t.product_type_param, '$.productTypeId') as uint256) as product_type_id_input,
    json_extract_scalar(t.product_type_param, '$.productTypeName') as product_type_name,
    json_extract_scalar(t.product_type_param, '$.ipfsMetadata') as call_product_type_ipfs_metadata,
    json_parse(json_query(t.product_type_param, 'lax $.productType' omit quotes)) as product_type_json,
    t.product_type_ordinality,
    pt.tx_hash
  from product_type_calls pt
    cross join unnest (pt.productTypeParams) with ordinality as t(product_type_param, product_type_ordinality)
  where pt.tx_call_rn = 1
),

product_type_data as (
  select
    block_time,
    block_number,
    product_type_id_input,
    product_type_name,
    try_cast(json_extract_scalar(product_type_json, '$.claimMethod') as int) as claim_method,
    try_cast(json_extract_scalar(product_type_json, '$.gracePeriod') as bigint) as grace_period,
    call_product_type_ipfs_metadata,
    product_type_ordinality,
    tx_hash,
    row_number() over (partition by block_time, tx_hash order by product_type_ordinality) as call_rn
  from product_type_data_raw
),

product_types_ext as (
  select
    e.block_time,
    e.block_number,
    e.product_type_id,
    c.product_type_name,
    c.claim_method,
    c.grace_period,
    coalesce(e.evt_product_type_ipfs_metadata, c.call_product_type_ipfs_metadata) as product_type_ipfs_metadata,
    e.evt_index,
    e.tx_hash
  from product_type_events e
    inner join product_type_data c on e.block_time = c.block_time and e.block_number = c.block_number and e.evt_rn = c.call_rn
  union all
  select
    block_time,
    block_number,
    product_type_id_input as product_type_id,
    product_type_name,
    claim_method,
    grace_period,
    call_product_type_ipfs_metadata as product_type_ipfs_metadata,
    cast(0 as bigint) as evt_index,
    tx_hash
  from product_type_data
  where tx_hash = 0x9d6219f34e1474a788c4ece1a14b18d8402653ff34f792ca088c344b80a93bd9 -- exception without event logs
),

product_types as (
  select
    block_time,
    block_number,
    cast(product_type_id as int) as product_type_id,
    if(
      product_type_name <> '',
      product_type_name,
      lag(product_type_name) over (partition by product_type_id order by block_time)
    ) as product_type_name,
    claim_method,
    grace_period,
    if(
      product_type_ipfs_metadata <> '',
      product_type_ipfs_metadata,
      lag(product_type_ipfs_metadata) over (partition by product_type_id order by block_time)
    ) as product_type_ipfs_metadata,
    evt_index,
    tx_hash,
    row_number() over (partition by product_type_id order by block_time desc) as rn
  from product_types_ext
)

select
  block_time,
  block_number,
  product_type_id,
  product_type_name,
  claim_method,
  grace_period,
  product_type_ipfs_metadata,
  concat('https://api.nexusmutual.io/ipfs/', product_type_ipfs_metadata) as cover_wording_url,
  evt_index,
  tx_hash
from product_types
where rn = 1
