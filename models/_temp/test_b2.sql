{{ config(
        schema = 'temp',
        alias = 'test_b',
        materialized = 'table'
        )
}}


select
    l.blockchain
    ,l.address
    ,l.custody_owner
    ,l.account_owner
    ,count(*) filter (where "to" = from_hex(l.address)) as transfers_in
    ,sum(amount_usd) filter (where "to" = from_hex(l.address)) as usd_in
    ,count(*) filter (where "from" = from_hex(l.address)) as transfers_out
    ,sum(amount_usd) filter (where "from" = from_hex(l.address)) as usd_out
from {{source('labels','owner_addresses')}} l
left join {{ref('tokens_ethereum_transfers')}} t
on t.blockchain = l.blockchain
-- and ("to" = from_hex(l.address) OR "from" = from_hex(l.address))
 and "to" = from_hex(l.address)
group by 1,2,3,4
