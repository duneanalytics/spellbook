{{ config(
        schema='prices',
        alias = 'usd_latest_historical',
        
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
    AND pu.contract_address != 0xd31fcd1f7ba190dbc75354046f6024a9b86014d7 -- remove bad price feed
{% endif %}
GROUP BY 1,2,3,4,5
