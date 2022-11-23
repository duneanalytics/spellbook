{{ config(
        alias ='listings_over_time',
        unique_key='punk_id',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cryptopunks",
                                    \'["cat"]\') }}'
        )
}}

with all_listings as (
    select  `punkIndex` as punk_id
            , 'Listing' as event_type
            , case when `toAddress` = '0x0000000000000000000000000000000000000000' then 'Public Listing'
                    else 'Private Listing'
                end as event_sub_type
            , `minValue`/1e18 as listed_price
            , `toAddress` as listing_offered_to 
            , evt_block_number
            , evt_index
            , evt_block_time
            , evt_tx_hash
    from {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkOffered') }}
)
, all_no_longer_for_sale_events (
    select  `punkIndex` as punk_id
            , 'No Longer For Sale' as event_type
            , case when evt_tx_hash in (select evt_tx_hash from cryptopunks_ethereum.CryptoPunksMarket_evt_PunkBought) then 'Punk Bought'
                    else 'Other'
                end as event_sub_type
            , null as listed_price
            , null as listing_offered_to
            , evt_block_number
            , evt_index
            , evt_block_time
            , evt_tx_hash
    from {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkNoLongerForSale') }}
)
, all_buys as (
    select  `punkIndex` as punk_id
            , 'Punk Bought' as event_type
            , 'Punk Bought' as event_sub_type
            , null as listed_price
            , null as listing_offered_to
            , evt_block_number
            , evt_index
            , evt_block_time
            , evt_tx_hash
    from {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkBought') }}
)
, all_transfers as (
    select  `punkIndex` as punk_id
            , 'Punk Transfer' as event_type
            , 'Punk Transfer' as event_sub_type
            , null as listed_price
            , null as listing_offered_to
            , evt_block_number
            , evt_index
            , evt_block_time
            , evt_tx_hash
    from {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkTransfer') }}
)
, base_data as (
    with all_days  as (select explode(sequence(to_date('2017-06-23'), to_date(now()), interval 1 day)) as day)
    , all_punk_ids as (select explode(sequence(0, 9999, 1)) as punk_id)
    
    select  day
            , punk_id
    from all_days
    full outer join all_punk_ids on true
)
, all_punk_events as (
    select *
          , row_number() over (partition by punk_id order by evt_block_number asc, evt_index asc ) as punk_event_index
    from 
    (
    select * from all_listings
    union all select * from all_no_longer_for_sale_events
    union all select * from all_buys
    union all select * from all_transfers
    ) a 
    order by evt_block_number desc, evt_index desc 
)
, aggregated_punk_on_off_data as (
    select date_trunc('day',a.evt_block_time) as day 
            , a.punk_id 
            , case when event_type = 'Listing' then 'Active' else 'Not Listed' end as listed_bool
    from all_punk_events a 
    inner join (    select date_trunc('day', evt_block_time) as day 
                            , punk_id
                            , max(punk_event_index) as max_event
                    from all_punk_events
                    group by 1,2
                ) b -- max event per punk per day 
    on date_trunc('day',a.evt_block_time) = b.day and a.punk_id = b.punk_id and a.punk_event_index = b.max_event
)
select day 
        , sum(case when bool_fill_in = 'Active' then 1 else 0 end) as listed_count
from 
(   select c.*
            , last_value(listed_bool,true) over (partition by punk_id order by day asc ) as bool_fill_in
    from 
    (   select a.day
                , a.punk_id 
                , listed_bool 
        from base_data  a
        left outer join aggregated_punk_on_off_data  b 
        on a.day = b.day and a.punk_id = b.punk_id
    ) c 
) d 
group by 1 
order by day desc 