{{ config(
        alias ='erc20_agg_hour', 
        materialized ='incremental', 
        file_format ='delta', 
        incremental_strategy='merge'
        )
}}

select
    'ethereum' as blockchain,
    date_trunc('hour', tr.evt_block_time) as hour,
    tr.wallet_address,
    tr.token_address,
    t.symbol,
    sum(tr.amount_raw) as amount_raw,
    sum(tr.amount_raw / power(10, t.decimals)) as amount
from {{ ref('transfers_ethereum_erc20') }} tr
left join {{ ref('tokens_ethereum_erc20') }} t on t.contract_address = tr.token_address
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where date_trunc('hour', tr.evt_block_time) > (select max(hour) from {{ this }})
{% endif %}
group by
    date_trunc('hour', tr.evt_block_time), tr.wallet_address, tr.token_address, t.symbol
