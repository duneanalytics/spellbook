{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'covers_v1',
    materialized = 'table',
    tags = ['static'],
    unique_key = ['cover_id'],
    post_hook = '{{ expose_spells(blockchains = \'["ethereum"]\',
                                  spell_type = "project",
                                  spell_name = "nexusmutual",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with

cover_details as (
  select
    cde.evt_block_time as block_time,
    cde.evt_block_number as block_number,
    cde.cid as cover_id,
    cde.evt_block_time as cover_start_time,
    from_unixtime(cde.expiry) as cover_end_time,
    cde.scAdd as product_contract,
    cast(cde.sumAssured as double) as sum_assured,
    case
      when ct.call_success or cm.payWithNXM then 'NXM'
      when cde.curr = 0x45544800 then 'ETH'
      when cde.curr = 0x44414900 then 'DAI'
    end as premium_asset,
    cde.premium / 1e18 as premium,
    case
      when cde.curr = 0x45544800 then 'ETH'
      when cde.curr = 0x44414900 then 'DAI'
    end as cover_asset,
    cde.premiumNXM / 1e18 as premium_nxm,
    ac._userAddress as cover_owner,
    cde.evt_index,
    cde.evt_tx_hash as tx_hash
  from {{ source('nexusmutual_ethereum', 'QuotationData_evt_CoverDetailsEvent') }} cde
    left join {{ source('nexusmutual_ethereum', 'Quotation_call_buyCoverWithMetadata') }} cm
      on cde.evt_tx_hash = cm.call_tx_hash and cde.evt_block_number = cm.call_block_number and cm.call_success
    left join {{ source('nexusmutual_ethereum', 'Quotation_call_makeCoverUsingNXMTokens') }} ct
      on cde.evt_tx_hash = ct.call_tx_hash and cde.evt_block_number = ct.call_block_number and ct.call_success
    left join {{ source('nexusmutual_ethereum', 'QuotationData_call_addCover') }} ac
      on cde.evt_tx_hash = ac.call_tx_hash and cde.evt_block_number = ac.call_block_number and ac.call_success
)

select
  cd.block_time,
  date_trunc('day', cd.block_time) as block_date,
  cd.block_number,
  cd.cover_id,
  cd.cover_start_time,
  cd.cover_end_time,
  date_trunc('day', cd.cover_start_time) as cover_start_date,
  date_trunc('day', cd.cover_end_time) as cover_end_date,
  cd.product_contract,
  'v1' as syndicate,
  cast(null as int) as product_id,
  coalesce(p.product_name, 'unknown') as product_name,
  coalesce(p.product_type, 'unknown') as product_type,
  cd.cover_asset,
  cd.sum_assured,
  cd.premium_asset,
  cd.premium,
  cd.premium_nxm,
  cd.cover_owner,
  cd.evt_index,
  cd.tx_hash
from cover_details cd
  left join {{ ref('nexusmutual_ethereum_products_v1') }} p on cd.product_contract = p.product_contract_address
