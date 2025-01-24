{{ config(
        schema='prices_v2',
        alias = 'hour_raw',
        materialized = 'incremental',
        file_format = 'delta',
        partition_by = ['date'],
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'contract_address', 'timestamp'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.timestamp')]
        )
}}


SELECT
    blockchain
    , contract_address
    , date_trunc('hour',timestamp) as timestamp
    , max_by(price,timestamp) as price
    , sum(volume) as volume
    , max_by(source,timestamp) as source
    , date
    , max(timestamp) as source_timestamp
FROM {{ ref('prices_v2_minute_sparse') }}
{% if is_incremental() %}
WHERE {{ incremental_predicate("date_trunc('hour',timestamp)") }}   -- this makes sure we always proces full hours
{% endif %}
GROUP BY 1,2,3,7
