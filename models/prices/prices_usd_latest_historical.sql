{{ config(
        schema='prices',
        alias = alias('usd_latest_historical'),
        tags= ['dunesql'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['unique_key']
        )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['pu.blockchain', 'pu.contract_address', 'pu.decimals', 'pu.symbol']) }} as unique_key
, pu.blockchain
, pu.contract_address
, pu.decimals
, pu.symbol
, max(pu.minute) as minute
, max_by(pu.price, pu.minute) as price
FROM {{ source('prices', 'usd') }} pu
{% if is_incremental() %}
    WHERE minute >= date_trunc('day', now() - interval '2' day)
{% endif %}
GROUP BY 1,2,3,4,5
