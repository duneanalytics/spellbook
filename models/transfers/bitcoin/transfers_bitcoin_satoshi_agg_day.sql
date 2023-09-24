{{ config(
        schema = 'transfers_bitcoin',
        alias = alias('satoshi_agg_day'),
        tags = ['dunesql'],
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['day', 'wallet_address']
        )
}}

select
    'bitcoin' as blockchain,
    tr.block_date as day,
    tr.wallet_address,
    sum(tr.amount_raw) as amount_raw,
    sum(tr.amount_transfer_usd) as amount_transfer_usd
from {{ ref('transfers_bitcoin_satoshi') }} tr
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where tr.block_date >= date_trunc('day', now() - interval '7' day)
{% endif %}
group by 1, 2, 3
