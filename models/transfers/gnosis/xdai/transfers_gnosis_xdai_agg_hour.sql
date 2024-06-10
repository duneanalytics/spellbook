{{ config(
        
        alias = 'xdai_agg_hour',
        partition_by = ['block_month'],
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['block_hour', 'wallet_address', 'counterparty', 'token_address']
        )
}}

select
    tr.blockchain,
    date_trunc('hour', tr.block_time) as block_hour,
    block_month,
    tr.wallet_address,
    tr.counterparty,
    tr.token_address,
    'xDAI' as symbol,
    sum(tr.amount_raw) as amount_raw,
    sum(tr.amount_raw / power(10, 18)) as amount
FROM 
{{ ref('transfers_gnosis_xdai') }} tr
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE tr.block_time >= date_trunc('hour', now() - interval '3' Day)
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 6, 7