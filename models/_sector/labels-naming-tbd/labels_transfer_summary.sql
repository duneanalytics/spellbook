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
    ,l.address
    ,count(*) as transfers_in
    ,sum(amount_usd) as usd_in
    ,min_by(tx_hash, block_time) as first_tx_hash
    ,min(block_time) as first_block_time
    ,max_by(tx_hash, block_time) as last_tx_hash
    ,max(block_time) as last_block_time
from {{source('labels','owner_addresses')}} l
left join {{ref('tokens_ethereum_transfers')}} t
on t.blockchain = l.blockchain
 and "to" = from_hex(l.address)
group by 1,2
)

,stats_out as (
select
    l.blockchain
    ,l.address
    ,count(*) as transfers_out
    ,sum(amount_usd)  as usd_out
from {{source('labels','owner_addresses')}} l
left join {{ref('tokens_ethereum_transfers')}} t
on t.blockchain = l.blockchain
 and "from" = from_hex(l.address)
group by 1,2
)

select
    blockchain
    ,address
    ,l.custody_owner
    ,l.account_owner
    ,transfers_in
    ,transfers_out
    ,usd_in
    ,usd_out
    ,first_tx_hash
    ,last_tx_hash
from {{source('labels','owner_addresses')}} l
left join stats_in using (blockchain, address)
left join stats_out using (blockchain, address)
