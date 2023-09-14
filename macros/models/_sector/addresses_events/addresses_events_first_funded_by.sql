{% macro addresses_events_first_funded_by(blockchain, traces) %}

SELECT 
'{{blockchain}}' as blockchain
, et.to AS address
, MIN_BY(et."from", et.block_number) AS first_funded_by
, MIN(et.block_time) AS block_time
, MIN(et.block_number) AS block_number
, MIN_BY(et.tx_hash, et.block_number) AS tx_hash
FROM {{ source( blockchain , 'traces') }} et
WHERE et.success
AND (et.call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR et.call_type IS NULL)
AND CAST(et.value AS double) > 0
{% if is_incremental() %}
AND et.block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
GROUP BY et.to
{% endmacro %}