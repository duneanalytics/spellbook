-- https://dune.com/queries/981250
create or replace view aztec_v2.view_rollup_txn_fees as
with tx_fees_unnested as (
select rollupid
    , call_block_time
    , (unnest(assetIds)) as asset_id
    , (unnest(totalTxFees)) as total_tx_fee
from aztec_v2.rollups_parsed
)
select f.rollupid
    , f.call_block_time
    , f.asset_id
    , a.asset_address
    , a.symbol
    , a.decimals
    , f.total_tx_fee as total_tx_fee_raw
    , f.total_tx_fee * 1.0 / 10 ^ (a.decimals) as total_tx_fee_norm
from tx_fees_unnested f
left join aztec_v2.view_deposit_assets a on f.asset_id = a.asset_id
where f.asset_id <> 1073741824 -- assetID 1073741824 is null value