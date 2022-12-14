{{ config(
        alias ='owners_over_time',
        unique_key='day',
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                    "project",
                                    "cryptopunks",
                                    \'["cat"]\') }}'
        )
}}

with transfers as (    
    select  date_trunc('day',evt_block_time) as day 
            , from as wallet
            , count(*)*-1.0 as punk_balance
    from {{ref('cryptopunks_ethereum_punk_transfers')}} 
    group by 1,2
    
    union all 
    
    select  date_trunc('day',evt_block_time) as day 
            , to as wallet
            , count(*) as punk_balance
    from {{ref('cryptopunks_ethereum_punk_transfers')}} 
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
    with all_days as (select explode(sequence(to_date('2017-06-23'), to_date(now()), interval 1 day)) as day)
    , all_wallets as (select distinct wallet from punk_transfer_summary)
    
    select  day
            , wallet
    from all_days 
    full outer join all_wallets on true
)
, combined_table as (
    select base_data.day
            , base_data.wallet
            , coalesce(daily_transfer_sum,0) as daily_transfer_sum
        --     , sum(coalesce(daily_transfer_sum,0)) over (partition by base_data.wallet order by base_data.day) as holding
    from base_data
    left join punk_transfer_summary on base_data.day = punk_transfer_summary.day and base_data.wallet = punk_transfer_summary.wallet 
)
, cumulative_data as (
    select t1.day, t1.wallet, t1.daily_transfer_sum, sum(t2.daily_transfer_sum) as holding
    from combined_table t1
    inner join combined_table t2 on t1.day >= t2.day and t1.wallet = t2.wallet
    group by t1.day, t1.wallet, t1.daily_transfer_sum
)
    
select day
        , count(wallet) filter (where holding > 0) as unique_wallets
from cumulative_data
group by 1
order by day desc
;