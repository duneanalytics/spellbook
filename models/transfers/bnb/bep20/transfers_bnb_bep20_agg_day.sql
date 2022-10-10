{{ config(
        alias ='bep20_agg_day',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key=['wallet_address', 'token_address', 'day']
        )
}}

select
    'bnb' as blockchain,
    date_trunc('day', tr.evt_block_time) as day,
    tr.wallet_address,
    tr.token_address,
    t.symbol,
    sum(tr.amount_raw) as amount_raw,
    sum(tr.amount_raw / power(10, t.decimals)) as amount
from {{ ref('transfers_bnb_bep20') }} tr
left join {{ ref('tokens_bnb_bep20') }} t on t.contract_address = tr.token_address
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where tr.evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}
group by 1, 2, 3, 4, 5