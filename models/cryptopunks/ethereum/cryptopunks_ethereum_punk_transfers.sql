{{ config(
        alias ='punk_transfers',
        partition_by = ['evt_block_time_week'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['evt_block_time_week', 'punk_id', 'evt_tx_hash', 'evt_index']
        )
}}

select *
from 
(   select  from
            , to
            , evt_block_time
            , evt_block_time_week
            , evt_block_number
            , evt_index
            , punk_id
            , evt_tx_hash 
    from 
    (   select from 
                , to 
                , evt_block_time
                , date_trunc('week',evt_block_time) as evt_block_time_week
                , evt_block_number
                , evt_index
                , punkIndex as punk_id
                , evt_tx_hash
        from {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkTransfer') }} 
        {% if is_incremental() %} where evt_block_time >= date_trunc('day', now() - interval '1 week') {% endif %}    
    ) c 

    union all 

    select  '0x0000000000000000000000000000000000000000' as from
            , to
            , evt_block_time
            , date_trunc('week',evt_block_time) as evt_block_time_week
            , evt_block_number
            , evt_index
            , punkIndex as punk_id
            , evt_tx_hash
    from {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_Assign') }}
    {% if is_incremental() %} where evt_block_time >= date_trunc('day', now() - interval '1 week') {% endif %}

) d 
order by evt_block_number desc, evt_index desc 