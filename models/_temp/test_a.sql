{{ config(
        schema = 'temp',
        alias = 'test_a',
        materialized = 'table'
        )
}}

WITH stats_in as (
select
    l.blockchain
    ,l.address
    ,count(*) as transfers_in
    ,sum(amount_usd) as usd_in
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
from {{source('labels','owner_addresses')}} l
left join stats_in using (blockchain, address)
left join stats_out using (blockchain, address)
