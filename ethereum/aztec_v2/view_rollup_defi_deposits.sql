create or replace view aztec_v2.view_rollup_defi_deposits as

with bridge_rollups as (
select rollupid
    , (unnest(bridges)).*
    , (unnest(defiDepositSums)) as defi_deposit_sum
from aztec_v2.rollups_parsed
)
select b.* 
    , ia.symbol as input_a_symbol
    , oa.symbol as output_a_symbol
    , case when b.secondinputinuse then ib.symbol else null end as input_b_symbol
    , case when b.secondoutputinuse then ob.symbol else null end as output_b_symbol
    , defi_deposit_sum * 1.0 / 10 ^ (ia.decimals) as defi_deposit_sum_norm -- defi deposit sum is denominated in input asset a
    , ia.asset_address as input_a_address
    , oa.asset_address as output_a_address
    , case when b.secondinputinuse then ib.asset_address else null end as input_b_address
    , case when b.secondoutputinuse then ob.asset_address else null end as output_b_address
from bridge_rollups b
left join aztec_v2.view_deposit_assets ia on ia.asset_id = b.inputassetida
left join aztec_v2.view_deposit_assets oa on oa.asset_id = b.outputassetida
left join aztec_v2.view_deposit_assets ib on ib.asset_id = b.inputassetidb
left join aztec_v2.view_deposit_assets ob on ob.asset_id = b.outputassetidb
where addressid <> 0 -- address id 0 means no data here
;