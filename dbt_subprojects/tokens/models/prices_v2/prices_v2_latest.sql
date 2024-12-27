{{ config(
        schema='prices_v2',
        alias = 'latest',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'contract_address']
        )
}}


SELECT
    , blockchain
    , contract_address
    , max(timestamp) as timestamp
    , max_by(price,timestamp) as price
    , max_by(volume,timestamp) as volume
    , max_by(source,timestamp)
FROM {{ ref('prices_v2_minute_raw') }}
{% if is_incremental() %}
WHERE {{ incremental_predicate('timestamp') }}
{% endif %}
GROUP BY 1,2
