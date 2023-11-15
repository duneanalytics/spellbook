{{ config(
    schema = 'addresses_events_zksync'
    , alias = 'first_activity'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'append'
    , unique_key = ['address']
    )
}}

SELECT 'zksync' AS blockchain
, et."from" AS address
, MIN_BY(et."to", et.block_number) AS first_activity_to
, MIN(et.block_time) AS first_block_time
, MIN(et.block_number) AS first_block_number
, MIN_BY(et.hash, et.block_number) AS first_tx_hash
, MIN_BY(COALESCE(sig.function, CAST((bytearray_substring(et.data, 1, 4)) as VARCHAR)), et.block_number) as first_function
, MIN_BY(et.value/1e18, et.block_number) as first_{{native_symbol}}_transferred
FROM 
{{ source('zksync', 'transactions') }} et
LEFT JOIN (
    SELECT 
        DISTINCT id, 
        split_part(signature,'(',1) as function 
    FROM 
    {{ ref('signatures') }} 
    where type = 'function_call'
    AND id NOT IN (0x09779838, 0x00000000) -- for some weird reason these have duplicates functions
) sig 
    ON sig.id = bytearray_substring(et.data, 1, 4)
{% if is_incremental() %}
LEFT JOIN {{this}} ffb ON et."from" = ffb.address 
WHERE ffb.address IS NULL
AND et.block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

GROUP BY et."from"
