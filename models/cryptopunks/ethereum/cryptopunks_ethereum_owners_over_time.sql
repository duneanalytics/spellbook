{{ config(
        
        alias = 'owners_over_time',
        unique_key='day',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cryptopunks",
                                    \'["cat"]\') }}'
        )
}}

with transfers_sub_table as (
    select  "from"
            , to
            , evt_block_time
            , evt_block_time_week
            , evt_block_number
            , punk_id
            , evt_tx_hash 
    from {{ref('cryptopunks_ethereum_punk_transfers')}} 
    group by 1,2,3,4,5,6,7
)
, transfers as (    
    select  date_trunc('day',evt_block_time) as day 
            , "from" as wallet
            , count(*)*-1.0 as punk_balance
    from transfers_sub_table
    group by 1,2
    
    union all 
    
    select  date_trunc('day',evt_block_time) as day 
            , to as wallet
            , count(*) as punk_balance
    from transfers_sub_table
    group by 1,2
)
, punk_transfer_summary as (
    select  day
            , wallet
            , sum(punk_balance) as daily_transfer_sum
    from transfers 
    group by day, wallet
)
, base_data as (
    with all_days as (select col as day from unnest(sequence(date('2017-06-23'), date(now()), interval '1' day)) as _u(col)) 
    , all_wallets as (select distinct wallet from punk_transfer_summary)
    
    select  day
            , wallet
    from all_days 
    full outer join all_wallets on true
)
, combined_table as (
    select base_data.day
            , base_data.wallet
            , sum(coalesce(daily_transfer_sum,0)) over (partition by base_data.wallet order by base_data.day) as holding
    from base_data
    left join punk_transfer_summary on base_data.day = punk_transfer_summary.day and base_data.wallet = punk_transfer_summary.wallet 
)
    
select day
        , count(wallet) filter (where holding > 0) as unique_wallets
from combined_table
group by 1
order by day desc
