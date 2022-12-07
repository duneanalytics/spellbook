{{ config(
        alias ='punk_bid_events',
        partition_by = ['evt_block_time_week'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['evt_block_time_week', 'evt_tx_hash', 'evt_index']
        
        )
}}


select  'Bid Entered' as event_type
        , fromAddress as bidder
        , punkIndex as punk_id
        , value/1e18 as eth_value
        , evt_block_time
        , date_trunc('week',evt_block_time) as evt_block_time_week
        , evt_block_number
        , evt_index
        , evt_tx_hash 
from {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkBidEntered') }}
{% if is_incremental() %} where evt_block_time >= date_trunc('day', now() - interval '1 week') {% endif %}  

union all 

select  'Bid Withdrawn' as event_type
        , fromAddress as bidder
        , punkIndex as punk_id
        , value/1e18 as eth_value
        , evt_block_time
        , date_trunc('week',evt_block_time) as evt_block_time_week
        , evt_block_number
        , evt_index
        , evt_tx_hash 
from {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkBidWithdrawn') }}
{% if is_incremental() %} where evt_block_time >= date_trunc('day', now() - interval '1 week') {% endif %}  
