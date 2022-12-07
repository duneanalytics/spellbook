{{ config(
        alias ='punk_offer_events',
        partition_by = ['evt_block_time_week'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['evt_block_time_week', 'evt_tx_hash', 'evt_index'] 
        )
}}


select punkIndex as punk_id
        , 'Offered' as event_type
        , b.from 
        , case when toAddress = '0x0000000000000000000000000000000000000000' then cast(NULL as varchar(5))
            else toAddress end as to
        , minValue/1e18 as eth_price
        , a.evt_block_time 
        , date_trunc('week',a.evt_block_time) as evt_block_time_week
        , a.evt_block_number
        , a.evt_index
        , a.evt_tx_hash
from {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkOffered') }} a
inner join {{ source('ethereum','transactions') }} b on a.evt_tx_hash = b.hash 
{% if is_incremental() %}
where a.evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}

union all 

select  a.punkIndex
        , 'Offer Withdrawn' as event_type
        , b.from  
        , cast(NULL as varchar(5)) as to 
        , cast(NULL as double) as eth_price
        , a.evt_block_time
        , date_trunc('week',a.evt_block_time) as evt_block_time_week
        , a.evt_block_number
        , a.evt_index
        , a.evt_tx_hash
from {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkNoLongerForSale') }} a
inner join {{ source('ethereum','transactions') }} b on a.evt_tx_hash = b.hash 
where evt_tx_hash not in (select distinct tx_hash from {{ ref('cryptopunks_ethereum_trades') }} )
{% if is_incremental() %}
and a.evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}