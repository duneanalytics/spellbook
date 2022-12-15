{{ config(
        alias ='punk_transfers_agg_day',
        partition_by = ['evt_block_time_week'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['evt_block_time_week', 'punk_id', 'evt_tx_hash', 'evt_index']
        )
}}


select  date_trunc('day',evt_block_time) as day 
        , from as wallet
        , count(*)*-1.0 as punk_balance
from {{ref('cryptopunks_ethereum_punk_transfers')}} 
{% if is_incremental() %}
where evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}
group by 1,2

union all 

select  date_trunc('day',evt_block_time) as day 
        , to as wallet
        , count(*) as punk_balance
from {{ref('cryptopunks_ethereum_punk_transfers')}} 
{% if is_incremental() %}
where evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}
group by 1,2
