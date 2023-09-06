{{ config(
        tags = ['dunesql'],
        alias = alias('matic_agg_hour'),
        partition_by = ['block_month'],
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['hour', 'wallet_address', 'token_address']
        )
}}

select
    tr.blockchain,
    date_trunc('hour', tr.block_time) as hour,
    block_month,
    tr.wallet_address,
    tr.token_address,
    'MATIC' as symbol,
    sum(tr.amount_raw) as amount_raw,
    sum(tr.amount_raw / power(10, 18)) as amount
FROM 
{{ ref('transfers_polygon_matic') }} tr
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE tr.block_time >= date_trunc('hour', now() - interval '3' Day)
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 6