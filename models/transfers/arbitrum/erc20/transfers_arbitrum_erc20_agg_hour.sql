{{ config(
        
        alias = 'erc20_agg_hour',
        materialized ='incremental',
        partition_by = ['block_month'],
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['block_hour', 'wallet_address', 'token_address']
        )
}}

select
    tr.blockchain,
    date_trunc('hour', tr.evt_block_time) as block_hour,
    block_month,
    tr.wallet_address,
    tr.token_address,
    t.symbol,
    sum(tr.amount_raw) as amount_raw,
    sum(tr.amount_raw / power(10, t.decimals)) as amount
FROM 
{{ ref('transfers_arbitrum_erc20') }} tr
LEFT JOIN 
{{ source('tokens_arbitrum', 'erc20') }} t on t.contract_address = tr.token_address
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE tr.evt_block_time >= date_trunc('hour', now() - interval '3' Day)
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 6