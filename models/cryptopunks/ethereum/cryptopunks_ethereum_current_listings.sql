{{ config(
        alias ='current_listings',
        unique_key='punk_id',
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
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

select b.punk_id
        , listed_price
        , evt_block_time as listing_created_at
from 
(
    select *
            , row_number() over (partition by punk_id order by evt_block_number desc, evt_index desc ) as punk_event_index
    from 
    (
    select * from all_listings
    union all select * from all_no_longer_for_sale_events
    union all select * from all_buys
    union all select * from all_transfers
    ) a 
) b

where punk_event_index = 1 and event_type = 'Listing' and event_sub_type = 'Public Listing' 
order by listed_price asc, evt_block_time desc 