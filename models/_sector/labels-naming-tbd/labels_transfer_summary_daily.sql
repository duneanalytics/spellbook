{{ config(
        schema = 'labels',
        alias = 'transfer_summary_daily',
        partition_by = ['blockchain'],
        materialized = 'table'
        )
}}

WITH stats_in as (
select
    l.blockchain
    ,l.address
    ,t.block_date
    ,count(*) as transfers_in
    ,sum(amount_usd) as usd_in
from {{source('labels','owner_addresses')}} l
left join {{ref('tokens_transfers')}} t
on t.blockchain = l.blockchain
 and "to" = from_hex(l.address)
group by 1,2,3
)

,stats_out as (
select
    l.blockchain
    ,l.address
    ,t.block_date
    ,count(*) as transfers_out
    ,sum(amount_usd)  as usd_out
from {{source('labels','owner_addresses')}} l
left join {{ref('tokens_transfers')}} t
on t.blockchain = l.blockchain
 and "from" = from_hex(l.address)
group by 1,2,3
)

select
    l.blockchain
    ,l.address
    ,l.custody_owner
    ,l.account_owner
    ,coalesce(stats_in.block_date,stats_out.block_date) as block_date
    ,transfers_in
    ,transfers_out
    ,usd_in
    ,usd_out
from {{source('labels','owner_addresses')}} l
left join stats_in
on l.blockchain = stats_in.blockchain
    and l.address = stats_in.address
left join stats_out
on l.blockchain = stats_out.blockchain
    and l.address = stats_out.address
    and (stats_in.block_date is null or stats_in.block_date = stats_out.block_date)
