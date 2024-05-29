{{ config(
        schema = 'labels',
        alias = 'transfer_summary_daily',
        partition_by = ['blockchain'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'address', 'block_date'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
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
    ,t.block_date
    ,count(*) as transfers_in
    ,sum(amount_usd) as usd_in
from {{source('labels','owner_addresses')}} l
inner join {{ref('tokens_transfers')}} t
on t.blockchain = l.blockchain
 and "to" = l.address
 and amount_usd < pow(10,12)
 {% if is_incremental() %}
 and {{ incremental_predicate('block_time') }}
 {% endif %}
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
inner join {{ref('tokens_transfers')}} t
on t.blockchain = l.blockchain
 and "from" = l.address
 and amount_usd < pow(10,12)
 {% if is_incremental() %}
 and {{ incremental_predicate('block_time') }}
 {% endif %}
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
where coalesce(stats_in.block_date,stats_out.block_date) is not null
