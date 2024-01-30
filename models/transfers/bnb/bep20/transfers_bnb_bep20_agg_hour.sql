{{ config(
        
        alias = 'bep20_agg_hour',
        partition_by = ['block_month'],
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['hour', 'wallet_address', 'token_address']
        )
}}

select
    tr.blockchain,
    date_trunc('hour', tr.evt_block_time) as hour,
    tr.block_month,
    tr.wallet_address,
    tr.token_address,
    t.symbol,
    sum(tr.amount_raw) as amount_raw,
    sum(tr.amount_raw / power(10, t.decimals)) as amount
FROM 
{{ ref('transfers_bnb_bep20') }} tr
LEFT JOIN 
{{ source('tokens_bnb', 'bep20') }} t on t.contract_address = tr.token_address
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE tr.evt_block_time >= date_trunc('hour', now() - interval '3' Day)
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 6