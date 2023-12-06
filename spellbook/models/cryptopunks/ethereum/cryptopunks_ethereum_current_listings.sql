{{ config(
        
        alias = 'current_listings',
        unique_key='punk_id',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cryptopunks",
                                    \'["cat"]\') }}'
        )
}}

with all_listing_events as (
    select  punk_id
            , event_type
            , case  when event_type = 'Offered' and to is null then 'Public Listing'
                    when event_type = 'Offered' and to is not null then 'Private Listing'
                else 'Listing Withdrawn' end as event_sub_type
            , eth_amount as listed_price
            , to as listing_offered_to
            , evt_block_number
            , evt_index
            , evt_block_time
            , evt_tx_hash
    from {{ ref('cryptopunks_ethereum_punk_offer_events') }}
)
, all_buys as (
    select  nft_token_id as punk_id
            , 'Punk Bought' as event_type
            , 'Punk Bought' as event_sub_type
            , cast(NULL as double) as listed_price
            , cast(NULL as varbinary) as listing_offered_to
            , block_number as evt_block_number
            , sub_tx_trade_id as evt_index
            , block_time as evt_block_time
            , tx_hash as evt_tx_hash
    from {{ ref('cryptopunks_ethereum_base_trades') }}
    where project = 'cryptopunks'
)
, all_transfers as (
    select  punk_id
            , 'Punk Transfer' as event_type
            , 'Punk Transfer' as event_sub_type
            , cast(NULL as double) as listed_price
            , cast(NULL as varbinary) as listing_offered_to
            , evt_block_number
            , evt_index
            , evt_block_time
            , evt_tx_hash
    from {{ ref('cryptopunks_ethereum_punk_transfers') }}
)
, latest_eth_price as (
    select price
    from {{ source('prices', 'usd') }}
    where blockchain = 'ethereum'
        and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    order by minute desc limit 1
)

select  punk_id
        , listed_price as listed_price_eth
        , listed_price * (select price from latest_eth_price) as listed_price_current_usd
        , evt_block_time as listing_created_at
from
(
    select *
            , row_number() over (partition by punk_id order by evt_block_number desc, evt_index desc) as punk_event_index
    from
    (   select * from all_listing_events
        union all select * from all_buys
        union all select * from all_transfers
    ) a
) b
where punk_event_index = 1 and event_type = 'Offered' and event_sub_type = 'Public Listing'
order by listed_price asc, evt_block_time desc
