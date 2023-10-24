{{ config(
        
        alias = 'bnb_agg_day',
        partition_by = ['block_month'],
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['day', 'wallet_address', 'token_address']
        )
}}

select
    tr.blockchain,
    CAST(date_trunc('day', tr.block_time) as date) as day,
    block_month,
    tr.wallet_address,
    tr.token_address,
    'BNB' as symbol,
    sum(tr.amount_raw) as amount_raw,
    sum(tr.amount_raw / power(10, 18)) as amount
FROM 
{{ ref('transfers_bnb_bnb') }} tr
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE tr.block_time >= date_trunc('day', now() - interval '3' Day)
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 6