{{ config(
        schema = 'labels',
        alias = 'transfer_summary',
        partition_by = ['blockchain'],
        materialized = 'table'
        )
}}

WITH stats_in as (
select
    l.blockchain
    ,from_hex(l.address) as address
    ,l.custody_owner
    ,l.account_owner
--    ,count(*) filter (where "from" = from_hex(l.address)) as transfers_out
    ,count(*) as transfers_in
--    ,sum(amount_usd) filter (where "from" = from_hex(l.address)) as usd_out
    ,sum(amount_usd) as usd_in
    ,min_by(tx_hash, block_time) as first_tx_hash
    ,max_by(tx_hash, block_time) as last_tx_hash
from {{source('labels','owner_addresses')}} l
left join {{ref('tokens_ethereum_transfers')}} t
on t.blockchain = l.blockchain
 and "to" = from_hex(l.address)
group by 1,2,3,4
)

select * from stats_in

