-- Coverage invariant for high-volume Aptos stablecoin assets.
--
-- Purpose:
-- Ensure final transfers keep noncanonical USDC/USDT assets that are present in
-- base transfer lineage. These assets use both Coin v1 and FA v2 standards.
--
-- Failure interpretation:
-- Any returned row means the final enrichment layer dropped a required asset
-- even though upstream/base transfer rows exist for it.

with required_assets as (
  select asset_type
  from (
    values
      ('0x5e156f1207d0ebfa19a9eeff00d62a282278fb8719f4fab3a586a0a2c0fffbea::coin::T'),
      ('0xa2eda21a58856fda86451436513b867c97eecb4ba099da5775520e0f7492e852::coin::T'),
      ('0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDC'),
      ('0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDT'),
      ('0xe568e9322107a5c9ba4cbd05a630a5586aa73e744ada246c3efb0f4ce3e295f3'),
      ('0x2b3be0a97a73c87ff62cbdd36837a9fb5bbd1d7f06a73b7ed62ec15c5326c1b8')
  ) as t (asset_type)
),

base_counts as (
  select
    b.asset_type,
    count(*) as base_rows
  from {{ ref('tokens_aptos_base_transfers') }} b
  where b.asset_type in (
    select asset_type
    from required_assets
  )
  group by 1
),

transfer_counts as (
  select
    t.asset_type,
    count(*) as transfer_rows
  from {{ ref('tokens_aptos_transfers') }} t
  where t.asset_type in (
    select asset_type
    from required_assets
  )
  group by 1
)

select
  a.asset_type,
  coalesce(b.base_rows, 0) as base_rows,
  coalesce(t.transfer_rows, 0) as transfer_rows
from required_assets a
left join base_counts b
  on a.asset_type = b.asset_type
left join transfer_counts t
  on a.asset_type = t.asset_type
where coalesce(b.base_rows, 0) > 0
  and coalesce(t.transfer_rows, 0) = 0
