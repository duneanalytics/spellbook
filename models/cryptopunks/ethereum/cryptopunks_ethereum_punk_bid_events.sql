{{ config(
        alias = alias('punk_bid_events'),
        partition_by = ['evt_block_time_week'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['evt_block_time_week', 'evt_tx_hash', 'evt_index'] 
        )
}}

select  event_type
        , punk_id
        , bidder 
        , eth_amount
        , eth_amount * p.price as usd_amount
        , evt_block_time
        , evt_block_time_week
        , evt_block_number
        , evt_index
        , evt_tx_hash
from 
(       select  'Bid Entered' as event_type
                , punkIndex as punk_id
                , fromAddress as bidder
                , value/1e18 as eth_amount
                , evt_block_time
                , date_trunc('week',evt_block_time) as evt_block_time_week
                , evt_block_number
                , evt_index
                , evt_tx_hash 
        from {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkBidEntered') }}
        {% if is_incremental() %}
        where evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}

        union all 

        select  'Bid Withdrawn' as event_type
                , punkIndex as punk_id
                , fromAddress as bidder
                , value/1e18 as eth_amount
                , evt_block_time
                , date_trunc('week',evt_block_time) as evt_block_time_week
                , evt_block_number
                , evt_index
                , evt_tx_hash 
        from {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkBidWithdrawn') }}
        {% if is_incremental() %}
        where evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
) a 
left join {{ source('prices', 'usd') }} p on p.minute = date_trunc('minute', a.evt_block_time)
        and p.contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        and p.blockchain = 'ethereum'
        {% if is_incremental() %}
        and p.minute >= date_trunc('day', now() - interval '1 week')
        {% endif %}
;