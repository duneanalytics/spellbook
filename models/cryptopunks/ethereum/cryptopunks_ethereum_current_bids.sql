{{ config(
        
        alias = 'current_bids',
        unique_key='punk_id',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cryptopunks",
                                    \'["cat"]\') }}'
        )
}}

with combined_events_table as (
    select *
            , row_number() over (partition by punk_id order by evt_block_number desc, evt_index desc ) as punk_id_tx_rank
    from
    (    select  event_type
                , bidder
                , cast(NULL as varbinary) as transfer_from
                , cast(NULL as varbinary) as transfer_to
                , punk_id
                , eth_amount
                , evt_block_time
                , evt_block_number
                , evt_index
                , evt_tx_hash
        from  {{ ref('cryptopunks_ethereum_punk_bid_events') }}

        union all

        select  'Transfer' as event_type
                , cast(NULL as varbinary) as bidder
                , "from" as transfer_from
                , to as transfer_to
                , punk_id
                , cast(NULL as double) as eth_amount
                , evt_block_time
                , evt_block_number
                , evt_index
                , evt_tx_hash
        from  {{ ref('cryptopunks_ethereum_punk_transfers') }}

        union all

        select  trade_category as event_type
                , cast(NULL as varbinary) as bidder
                , seller as transfer_from
                , buyer as transfer_to
                , nft_token_id as punk_id
                , price_raw/pow(10,18) as eth_amount
                , block_time
                , block_number
                , sub_tx_trade_id as evt_index
                , tx_hash
        from  {{ ref('cryptopunks_ethereum_base_trades') }}
        WHERE project = 'cryptopunks'
    ) a
)
, min_event_per_punk as (
    select  punk_id
            , event_type
            , min(punk_id_tx_rank) as min_punk_event_rank
    from combined_events_table
    group by 1,2
)
, latest_eth_price as (
    select price
    from {{ source('prices', 'usd') }}
    where blockchain = 'ethereum'
        and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    order by minute desc limit 1
)

select  bidder
        , punk_id
        , eth_amount as bid_amount_eth
        , eth_amount * (select price from latest_eth_price) as bid_amount_current_usd
        , evt_block_time
        , evt_block_number
        , evt_index
        , evt_tx_hash
from
(   select a.event_type
            , a.bidder
            , a.transfer_from
            , a.transfer_to
            , a.punk_id
            , a.eth_amount
            , a.evt_block_time
            , a.evt_block_number
            , a.evt_index
            , a.evt_tx_hash
            , a.punk_id_tx_rank
            , b2.min_punk_event_rank as most_recent_offer_accept
            , b4.min_punk_event_rank as most_recent_bid
            , b5.min_punk_event_rank as most_recent_withdraw
            , array_join(ARRAY_AGG(c1.transfer_to), ',') as buyers_post_bid
            , array_join(ARRAY_AGG(c2.transfer_to), ',') as transfers_post_bid
    from combined_events_table a
    left outer join min_event_per_punk b2 on b2.event_type = 'Offer Accepted' and b2.punk_id = a.punk_id
    left outer join min_event_per_punk b4 on b4.event_type = 'Bid Entered' and b4.punk_id = a.punk_id
    left outer join min_event_per_punk b5 on b5.event_type = 'Bid Withdrawn' and b5.punk_id = a.punk_id

    left outer join combined_events_table c1 on c1.event_type = 'Buy' and a.punk_id = c1.punk_id and c1.punk_id_tx_rank < b4.min_punk_event_rank
    left outer join combined_events_table c2 on c2.event_type = 'Transfer' and a.punk_id = c2.punk_id and c2.punk_id_tx_rank < b4.min_punk_event_rank

    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
) a
where a.punk_id_tx_rank = most_recent_bid -- pull the most recent bid for each punk
    and (   a.punk_id_tx_rank < most_recent_withdraw -- make sure it hasn't been withdrawn
            or most_recent_withdraw is null
        )
    and (   a.punk_id_tx_rank < most_recent_offer_accept -- bid accepted will reset bids
            or most_recent_offer_accept is null
        )
    and (   buyers_post_bid not like concat('%', cast(a.bidder as varchar), '%') -- if bidder buys punk, their open bid is cancelled
            or buyers_post_bid is null
        )
    and (   transfers_post_bid not like concat('%', cast(a.bidder as varchar), '%') -- if bidder transferred punk, their open bid is cancelled
            or transfers_post_bid is null
        )

order by evt_block_number desc, evt_index desc
