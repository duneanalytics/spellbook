-- https://dune.com/queries/1007310

drop view if exists aztec_v2.view_rollup_inner_proofs;

create or replace view aztec_v2.view_rollup_inner_proofs as 
with inner_proofs as (
    select rollupid
        , call_block_time
        , (unnest(innerproofs)).*
    from aztec_v2.rollups_parsed
)
select i.*
    , substring(publicowner from 13 for 20) as publicowner_norm -- publicowner is 32 bytes long, with 12 bytes of padding over the 20 bytes of ethereum address
    , a.symbol
    , a.asset_address
    , a.decimals
    , i.publicvalue * 1.0 / 10 ^ (coalesce(a.decimals, 18)) as publicvalue_norm
    , i.publicvalue * 1.0 / 10 ^ (coalesce(a.decimals, 18)) * p.avg_price_usd as publicvalue_usd
from inner_proofs i
left join aztec_v2.view_deposit_assets a on i.assetid = a.asset_id
left join aztec_v2.daily_token_prices p on a.asset_address = p.token_address
    and i.call_block_time::date = p.date
;