-- NB: this is a generated query, do not edit it directly, instead edit the template and re-generate the query
-- hydrate the generated ouput here: https://dune.com/queries/4403433
{{ paraswap_all_entities() }}
,ordered_entities as (
    select 
        entity, blockchain, contract_address, block_time, tx_hash
    from entities
    order by block_time, tx_hash
)
select 
    blockchain, 
    entity, 
    contract_address, 
    count(*) as qty, 
    -- a rubbish but tolerable way to get the checksum of the txhash. Is consistent with internal counterparty query though
    sum(varbinary_to_decimal(from_hex(substring(to_hex(tx_hash),1,0+8)))) as txhash_checksum
from ordered_entities
group by 1,2,3
order by qty desc