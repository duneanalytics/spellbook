{{ config(
        schema = 'labels',
        alias = 'transfer_summary',
        partition_by = ['blockchain'],
        materialized = 'table'
        )
}}



select
    l.blockchain
    ,l.address
    ,l.custody_owner
    ,l.account_owner
    ,count(*) filter (where "from" = l.address) as transfers_out
    ,count(*) filter (where "to" = l.address) as transfers_in
    ,sum(amount_usd) filter (where "from" = l.address) as usd_out
    ,sum(amount_usd) filter (where "to" = l.address) as usd_in
    ,min_by(tx_hash, block_time) as first_tx_hash
    ,max_by(tx_hash, block_time) as last_tx_hash
from {{ref('tokens_transfers')}} t
left join {{source('labels','owner_addresses')}} l
on t.blockchain = l.blockchain
 and ("from" = l.address or "to" = l.address)
group by 1,2,3,4
