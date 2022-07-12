-- https://dune.com/queries/981259
create or replace view aztec_v2.view_deposit_assets as
-- ETH is the default asset that doesn't need to be added
with assets_added as (
  select 0 as asset_id 
      , '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' as asset_address
      , null as asset_gas_limit
      , null as date_added
  union
  select "assetId" as asset_id
      , "assetAddress" as asset_address
      , "assetGasLimit" as asset_gas_limit
      , evt_block_time as date_added
  from aztec_v2."RollupProcessor_evt_AssetAdded"
)
select a.*
  , t.symbol
  , t.decimals
from assets_added a
left join erc20.tokens t on a.asset_address = t.contract_address
;