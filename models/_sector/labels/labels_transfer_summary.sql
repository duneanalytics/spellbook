{{ config(
        schema = 'labels',
        alias = 'transfer_summary',
        partition_by = ['blockchain'],
        materialized = 'table',
        file_format = 'delta',
        post_hook = '{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "celo", "ethereum", "fantom", "gnosis", "optimism", "polygon", "scroll", "zksync"]\',
                                    "sector",
                                    "labels",
                                    \'["0xRob"]\') }}'
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
inner join {{ref('tokens_transfers')}} t
on t.blockchain = l.blockchain
 and "to" = l.address
 and amount_usd < pow(10,12)
group by 1,2
)

,stats_out as (
select
    l.blockchain
    ,l.address
    ,count(*) as transfers_out
    ,sum(amount_usd)  as usd_out
    ,min_by(tx_hash, block_time) as first_tx_hash
    ,min(block_time) as first_block_time
    ,max_by(tx_hash, block_time) as last_tx_hash
    ,max(block_time) as last_block_time
from {{source('labels','owner_addresses')}} l
inner join {{ref('tokens_transfers')}} t
on t.blockchain = l.blockchain
 and "from" = l.address
 and amount_usd < pow(10,12)
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
    ,case when stats_in.first_block_time < stats_out.first_block_time then stats_in.first_tx_hash
        else stats_out.first_tx_hash end as first_tx_hash
    ,case when stats_in.last_block_time > stats_out.last_block_time then stats_in.last_tx_hash
        else stats_out.last_tx_hash end as last_tx_hash
from {{source('labels','owner_addresses')}} l
left join stats_in using (blockchain, address)
left join stats_out using (blockchain, address)
