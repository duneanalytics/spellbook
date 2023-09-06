{{ config(
        tags = ['dunesql'],
        alias = alias('matic_agg_day'),
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['day', 'wallet_address', 'token_address']
        )
}}

select
    tr.blockchain,
    CAST(date_trunc('day', tr.block_time) as date) as day,
    tr.wallet_address,
    tr.token_address,
    'MATIC' as symbol,
    sum(tr.amount_raw) as amount_raw,
    sum(tr.amount_raw / power(10, 18)) as amount
FROM 
{{ ref('transfers_polygon_matic_tfers') }} tr
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE tr.block_time >= date_trunc('day', now() - interval '7' Day)
{% endif %}
GROUP BY 1, 2, 3, 4, 5