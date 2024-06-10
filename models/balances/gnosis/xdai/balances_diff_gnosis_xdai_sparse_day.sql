{{ config(
        
        alias = 'xdai_diff_day',
        partition_by = ['block_month'],
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['block_day', 'wallet_address', 'token_address']
        )
}}


SELECT
    blockchain,
    date_trunc('day', block_time) AS block_day,
    block_month,
    wallet_address,
    token_address,
    'xDAI' AS symbol,
    ARRAY_AGG(amount_raw ORDER BY block_time DESC)[1] AS amount_raw,
    ARRAY_AGG(amount_raw / power(10, 18) ORDER BY block_time DESC)[1] AS amount,
    SUM(amount_raw_diff) AS amount_raw_diff,
    SUM(amount_raw_diff / power(10, 18)) AS amount_diff
FROM 
{{ ref('balances_gnosis_xdai_sparse') }}
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE block_time >= date_trunc('hour', now() - interval '3' Day)
{% endif %}
GROUP BY 
    1, 2, 3, 4, 5, 6