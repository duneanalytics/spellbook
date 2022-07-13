drop view if exists aztec_v2.view_rollup_defi_deposits;

create or replace view aztec_v2.view_rollup_defi_deposits as

with bridge_rollups as (
select rollupid
    , call_block_time
    , (unnest(bridges)).*
    , (unnest(defiDepositSums)) as defi_deposit_sum
from aztec_v2.rollups_parsed
)
, bridge_list as (
select "bridgeAddressId" as bridge_address_id
    , "bridgeAddress" as bridge_address
from aztec_v2."RollupProcessor_evt_BridgeAdded"
)
select b.rollupid
    , b.call_block_time
    , b.addressid
    , b.name as parser_encoded_name
    , bl.bridge_address
    , cl.protocol
    , cl.description
    , b.inputassetida
    , b.outputassetida
    , b.inputassetidb
    , b.outputassetidb
    , b.auxdata
    , b.secondinputinuse
    , b.secondoutputinuse
    , b.defi_deposit_sum
    , ia.symbol as input_a_symbol
    , oa.symbol as output_a_symbol
    , case when b.secondinputinuse then ib.symbol else null end as input_b_symbol
    , case when b.secondoutputinuse then ob.symbol else null end as output_b_symbol
    , defi_deposit_sum * 1.0 / 10 ^ (ia.decimals) as defi_deposit_sum_norm -- defi deposit sum is denominated in input asset a
    , defi_deposit_sum * 1.0 / 10 ^ (ia.decimals) * p.avg_price_usd as defi_deposit_sum_usd
    , ia.asset_address as input_a_address
    , oa.asset_address as output_a_address
    , case when b.secondinputinuse then ib.asset_address else null end as input_b_address
    , case when b.secondoutputinuse then ob.asset_address else null end as output_b_address
from bridge_rollups b
left join bridge_list bl on b.addressid = bl.bridge_address_id
left join aztec_v2.contract_labels cl on bl.bridge_address = cl.contract_address
left join aztec_v2.view_deposit_assets ia on ia.asset_id = b.inputassetida
left join aztec_v2.view_deposit_assets oa on oa.asset_id = b.outputassetida
left join aztec_v2.view_deposit_assets ib on ib.asset_id = b.inputassetidb
left join aztec_v2.view_deposit_assets ob on ob.asset_id = b.outputassetidb
left join aztec_v2.daily_token_prices p on ia.asset_address = p.token_address
    and b.call_block_time::date = p.date
where addressid <> 0 -- address id 0 means no data here
;