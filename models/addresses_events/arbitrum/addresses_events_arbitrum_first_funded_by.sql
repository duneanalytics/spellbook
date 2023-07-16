{{ config(
    schema = 'addresses_events_arbitrum'
    , alias = alias('first_funded_by')
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'append'
    , unique_key = ['address']
    )
}}

SELECT 'arbitrum' AS blockchain
, et.to AS address
, MIN_BY(et.from, et.block_number) AS first_funded_by
, MIN(et.block_time) AS block_time
, MIN(et.block_number) AS block_number
, MIN_BY(et.tx_hash, et.block_number) AS tx_hash
FROM {{ source('arbitrum', 'traces') }} et
{% if is_incremental() %}
LEFT ANTI JOIN {{this}} ffb ON et.to = ffb.address
{% endif %}
WHERE et.success
AND (et.call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR et.call_type IS NULL)
AND CAST(et.value AS double) > 0
{% if is_incremental() %}
AND et.block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}
GROUP BY et.to
;