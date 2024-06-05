{{ config(

        alias = 'bnb_agg_hour',
        partition_by = ['block_month'],
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.hour')],
        unique_key = ['hour', 'wallet_address', 'token_address']
        )
}}

select
    tr.blockchain,
    date_trunc('hour', tr.block_time) as hour,
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
WHERE {{ incremental_predicate('tr.block_time') }}
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 6