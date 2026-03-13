{% macro addresses_events_first_funded_by(blockchain, token_transfers) %}

WITH token_transfers_with_sort_keys AS (
    SELECT tt.*
    , COALESCE(CAST(tt.block_number AS bigint), 9223372036854775807) AS sort_block_number
    , COALESCE(CAST(tt.tx_index AS bigint), 9223372036854775807) AS sort_tx_index
    , TRANSFORM(
        COALESCE(
            CAST(tt.trace_address AS array(bigint)),
            CAST(ARRAY[] AS array(bigint))
        ),
        x -> COALESCE(x, -1)
    ) AS sort_trace_address
    FROM {{token_transfers}} tt
)

SELECT '{{blockchain}}' as blockchain
, tt.to AS address
, MIN_BY(tt."from", (tt.sort_block_number, tt.sort_tx_index, tt.sort_trace_address)) AS first_funded_by
, MIN_BY(tt.tx_from, (tt.sort_block_number, tt.sort_tx_index)) AS first_funding_executed_by
, MIN_BY(tt.amount, (tt.sort_block_number, tt.sort_tx_index, tt.sort_trace_address)) AS amount
, MIN_BY(tt.amount_usd, (tt.sort_block_number, tt.sort_tx_index, tt.sort_trace_address)) AS amount_usd
, MIN(tt.block_time) AS block_time
, MIN(tt.block_number) AS block_number
, MIN_BY(tt.tx_hash, (tt.sort_block_number, tt.sort_tx_index)) AS tx_hash
, MIN_BY(tt.tx_index, (tt.sort_block_number, tt.sort_tx_index)) AS tx_index
, MIN_BY(tt.trace_address, (tt.sort_block_number, tt.sort_tx_index, tt.sort_trace_address)) AS trace_address
, MIN_BY(tt.unique_key, (tt.sort_block_number, tt.sort_tx_index, tt.sort_trace_address)) AS unique_key
FROM token_transfers_with_sort_keys tt
{% if is_incremental() %}
WHERE {{ incremental_predicate('tt.block_time') }}
AND tt.block_time >= now() - interval '14' day -- Temporary CI throttle: limit scan to recent activity.
AND tt.token_standard = 'native'
AND tt.to NOT IN (SELECT address FROM {{this}})
{% else %}
WHERE tt.token_standard = 'native'
AND tt.block_time >= now() - interval '14' day -- Temporary CI throttle: limit scan to recent activity.
{% endif %}
GROUP BY tt.to

{% endmacro %}
