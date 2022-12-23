{{ config(
        alias ='ftm_agg_hour',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_transfer_id'
        )
}}

select
    'fantom' as blockchain,
    date_trunc('hour', tr.evt_block_time) as hour,
    tr.wallet_address,
    tr.wallet_address || '-' || date_trunc('hour', tr.evt_block_time) as unique_transfer_id,
    sum(tr.amount_raw) as amount_raw,
    sum(tr.amount_raw / power(10, 18)) as amount
from {{ ref('transfers_fantom_ftm') }} tr
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where tr.evt_block_time >= date_trunc('hour', now() - interval '1 week')
{% endif %}
group by 1, 2, 3, 4
