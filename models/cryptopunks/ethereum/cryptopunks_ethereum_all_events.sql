{{ config(
        alias ='all_events',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cryptopunks",
                                    \'["cat"]\') }}'
        )
}}

select *
from 
(
    select  punk_id
            , event_type
            , bidder as from 
            , cast(NULL as varchar(5)) as to 
            , eth_amount
            , evt_block_time
            , evt_block_number
            , evt_tx_hash
    from {{ ref('cryptopunks_ethereum_punk_bid_events') }}

    union all 

    select  punk_id
            , event_type
            , from 
            , to 
            , eth_amount
            , evt_block_time
            , evt_block_number
            , evt_tx_hash 
    from {{ ref('cryptopunks_ethereum_punk_offer_events') }}

    union all 

    select token_id 
            , 'Sold' as event_type
            , seller
            , buyer
            , amount_original
            , block_time
            , block_number 
            , tx_hash
    from {{ ref('cryptopunks_ethereum_trades') }}

    union all

    select punk_id 
            , case when evt_tx_hash is null then 'Claimed' else 'Transfer' end as trade_category
            , from
            , to
            , cast(NULL as double) as eth_value
            , evt_block_time
            , evt_block_number 
            , evt_tx_hash
    from {{ ref('cryptopunks_ethereum_punk_transfers') }}
    where (evt_tx_hash is null or evt_tx_hash not in (select distinct tx_hash from {{ ref('cryptopunks_ethereum_trades') }} ))
) a 
order by evt_block_number desc