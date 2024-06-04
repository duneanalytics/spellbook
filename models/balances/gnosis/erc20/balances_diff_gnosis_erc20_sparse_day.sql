{{ config(
        schema = 'balances_diff_gnosis',
        alias = 'erc20_day',
        post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "sector",
                                    "balances",
                                    \'["hdser"]\') }}',
        partition_by = ['block_month'],
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['block_hour', 'wallet_address', 'token_address']
        )
}}


SELECT
    blockchain,
    block_day,
    block_month,
    wallet_address,
    token_address,
    symbol,
    SUM(amount_raw) AS amount_raw,
    SUM(amount) AS amount
FROM 
{{ ref('transfers_gnosis_erc20_agg_day') }} 
{% if is_incremental() %}
WHERE block_hour >= date_trunc('hour', now() - interval '3' Day)
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 6