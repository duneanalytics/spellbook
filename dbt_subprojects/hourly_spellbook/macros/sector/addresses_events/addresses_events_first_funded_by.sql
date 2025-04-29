{% macro addresses_events_first_funded_by(blockchain, token_transfers) %}


SELECT '{{blockchain}}' as blockchain
, tt.to AS address
, MIN_BY(tt."from", (tt.block_number, tt.tx_index, COALESCE(tt.trace_address, ARRAY[]))) AS first_funded_by
, MIN_BY(tt.tx_from, (tt.block_number, tt.tx_index)) AS first_funding_executed_by
, MIN_BY(tt.amount, (tt.block_number, tt.tx_index, COALESCE(tt.trace_address, ARRAY[]))) AS amount
, MIN_BY(tt.amount_usd, (tt.block_number, tt.tx_index, COALESCE(tt.trace_address, ARRAY[]))) AS amount_usd
, MIN(tt.block_time) AS block_time
, MIN(tt.block_number) AS block_number
, MIN_BY(tt.tx_hash, (tt.block_number, tt.tx_index)) AS tx_hash
, MIN_BY(tt.tx_index, (tt.block_number, tt.tx_index)) AS tx_index
, MIN_BY(tt.trace_address, (tt.block_number, tt.tx_index, COALESCE(tt.trace_address, ARRAY[]))) AS trace_address
, MIN_BY(tt.unique_key, (tt.block_number, tt.tx_index, COALESCE(tt.trace_address, ARRAY[]))) AS unique_key
FROM {{token_transfers}} tt
{% if is_incremental() %}
WHERE {{ incremental_predicate('tt.block_time') }}
AND tt.token_standard = 'native'
AND tt.to NOT IN (SELECT address FROM {{this}})
{% else %}
WHERE tt.token_standard = 'native'
{% endif %}
GROUP BY tt.to

{% endmacro %}
