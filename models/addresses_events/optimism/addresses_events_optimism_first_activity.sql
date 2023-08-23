{{ config(
    schema = 'addresses_events_optimism'
    , tags = ['dunesql']
    , alias = alias('first_activity')
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'append'
    , unique_key = ['address']
    )
}}

SELECT 'optimism' AS blockchain
, et."from" AS address
, MIN_BY(et."to", et.block_number) AS first_activity_to
, MIN(et.block_time) AS block_time
, MIN(et.block_number) AS block_number
, MIN_BY(et.hash, et.block_number) AS tx_hash
, MIN_BY((bytearray_substring(et.data, 1, 4)), et.block_number) as first_function
FROM {{ source('optimism', 'transactions') }} et
{% if is_incremental() %}
LEFT JOIN {{this}} ffb ON et."from" = ffb.address WHERE ffb.address IS NULL
{% else %}
WHERE 1 = 1
{% endif %}
{% if is_incremental() %}
AND et.block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
GROUP BY et."from"
