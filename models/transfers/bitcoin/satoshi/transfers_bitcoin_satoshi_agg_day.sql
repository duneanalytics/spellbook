{{ config(
        alias = alias('satoshi_agg_day'),
        tags = ['dunesql'],
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_transfer_id'
        )
}}

select
    'bitcoin' as blockchain,
    block_date as day,
    tr.wallet_address,
    tr.wallet_address || '-' || block_date as unique_transfer_id,
    sum(tr.amount_raw) as amount_raw
from {{ ref('transfers_bitcoin_satoshi') }} tr
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where tr.evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}
group by 1, 2, 3, 4
