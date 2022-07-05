{{ config(
        alias ='erc20_agg_day',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='wallet_contract_day'
        )
}}

select
    'ethereum' as blockchain,
    date_trunc('day', tr.evt_block_time) as day,
    tr.wallet_address,
    tr.token_address,
    t.symbol,
    tr.wallet_address || '-' || tr.token_address || '-' || date_trunc('day', tr.evt_block_time) as wallet_contract_day,
    sum(tr.amount_raw) as amount_raw,
    sum(tr.amount_raw / power(10, t.decimals)) as amount
from {{ ref('transfers_ethereum_erc20') }} tr
left join {{ ref('tokens_ethereum_erc20') }} t on t.contract_address = tr.token_address
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where date_trunc('day', tr.evt_block_time) > now() - interval 2 days
{% endif %}
group by
    date_trunc('day', tr.evt_block_time),
  tr.wallet_address,
  tr.token_address,
  t.symbol,
  tr.wallet_address || '-' || tr.token_address || '-' || date_trunc('day', tr.evt_block_time)