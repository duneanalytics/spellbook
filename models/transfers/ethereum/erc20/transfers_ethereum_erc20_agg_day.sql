{{ config(
        tags = ['dunesql'],
        alias = alias('erc20_agg_day'),
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_transfer_id'
        )
}}

select
    'ethereum' as blockchain,
    date_trunc('day', tr.evt_block_time) as day,
    tr.wallet_address,
    tr.token_address,
    t.symbol,
    cast(tr.wallet_address as varchar) || '-' || cast(tr.token_address as varchar) || '-' || cast(date_trunc('day', tr.evt_block_time) as varchar) as unique_transfer_id,
    sum(case when tr.amount_positive then cast(tr.amount_raw as double) else - cast(tr.amount_raw as double) end) as amount_raw,
    sum(case when tr.amount_positive then cast(tr.amount_raw as double) / power(10, t.decimals) else - cast(tr.amount_raw as double) / power(10, t.decimals) end) as amount
from {{ ref('transfers_ethereum_erc20') }} tr
left join {{ ref('tokens_ethereum_erc20') }} t on t.contract_address = tr.token_address
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where tr.evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}
group by 1, 2, 3, 4, 5, 6
