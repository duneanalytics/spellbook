{% macro addresses_events_first_funded_by(blockchain, token_transfers) %}


{% if is_incremental() %}
WITH threshold AS (
    SELECT MAX(block_time) AS latest_block_time
    FROM {{this}}
    )
{% endif %}

SELECT '{{blockchain}}' as blockchain
, tt.to AS address
, MIN_BY(tt."from", (tt.block_number, tt.tx_index)) AS first_funded_by
, MIN_BY(tt.tx_from, (tt.block_number, tt.tx_index)) AS first_funding_executed_by
, MIN(tt.block_time) AS block_time
, MIN(tt.block_number) AS block_number
, MIN_BY(tt.tx_hash, (tt.block_number, tt.tx_index)) AS tx_hash
, MIN_BY(tt.tx_index, tt.block_number) AS tx_index
, MIN_BY(tt.unique_key, (tt.block_number, tt.tx_index)) AS unique_key
FROM {{token_transfers}} tt
{% if is_incremental() %}
INNER JOIN threshold t ON tt.block_time>t.latest_block_time
LEFT JOIN {{this}} t ON tt.to=t.address
    AND t.first_funded_by IS NULL
WHERE {{ incremental_predicate('tt.block_time') }}
AND tt.token_standard = 'native'
{% else %}
WHERE tt.token_standard = 'native'
{% endif %}
GROUP BY tt.to

{% endmacro %}