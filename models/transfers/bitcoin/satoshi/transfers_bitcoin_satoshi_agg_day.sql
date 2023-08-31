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
    tr.block_date as day,
    tr.wallet_address,
    tr.wallet_address || '-' || cast(block_date as varchar(10)) as unique_transfer_id,
    sum(tr.amount_raw) as amount_raw
from {{ ref('transfers_bitcoin_satoshi') }} tr
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where tr.block_date >= date(now()) - interval '7' day
{% endif %}
group by 1, 2, 3, 4
