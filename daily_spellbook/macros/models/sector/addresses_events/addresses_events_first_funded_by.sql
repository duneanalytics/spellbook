{% macro addresses_events_first_funded_by(blockchain) %}

SELECT '{{blockchain}}' as blockchain
, tt.to AS address
, MIN_BY(tt."from", tt.block_number) AS first_funded_by
, MIN_BY(tt.tx_from, tt.block_number) AS first_funding_executed_by
, MIN(tt.block_time) AS block_time
, MIN(tt.block_number) AS block_number
, MIN_BY(tt.tx_hash, tt.block_number) AS tx_hash
, MIN_BY(tt.tx_index, tt.block_number) AS tx_index
FROM {{token_transfers}} tt
{% if is_incremental() %}
LEFT JOIN {{this}} ffb ON tt.to = ffb.address WHERE ffb.address IS NULL
WHERE {{ incremental_predicate('tt.block_time') }}
AND tt.token_standard = 'native'
{% else %}
WHERE tt.token_standard = 'native'
{% endif %}
GROUP BY tt.to

{% endmacro %}