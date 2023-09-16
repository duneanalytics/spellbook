{{ config(
        schema = 'transfers_evms',
        alias = alias('erc20_agg_day'),
        tags = ['dunesql'],
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        partition_by = ['blockchain', 'day'],
        unique_key = ['blockchain', 'day', 'wallet_address', 'token_address', 'symbol']
        )
}}

select
    blockchain,
    tr.block_date as day,
    tr.wallet_address,
    tr.token_address,
    tr.symbol,
    sum(tr.amount_raw) as amount_raw,
    sum(tr.amount) as amount,
    sum(tr.amount_transfer_usd) as amount_transfer_usd
from {{ ref('transfers_evms_erc20') }} tr
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where tr.block_date >= date_trunc('day', now() - interval '7' day)
{% endif %}
group by 1, 2, 3, 4, 5
