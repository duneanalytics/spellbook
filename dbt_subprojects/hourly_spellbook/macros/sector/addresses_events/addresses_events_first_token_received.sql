{% macro addresses_events_first_token_received(blockchain, token_transfers) %}

WITH token_transfers_with_sort_keys AS (
    SELECT tt.*
    , COALESCE(CAST(tt.block_number AS bigint), 9223372036854775807) AS sort_block_number
    , COALESCE(CAST(tt.tx_index AS bigint), 9223372036854775807) AS sort_tx_index
    , TRANSFORM(
        COALESCE(
            CAST(tt.trace_address AS array(bigint)),
            ARRAY[COALESCE(CAST(tt.evt_index AS bigint), -1)]
        ),
        x -> COALESCE(x, -1)
    ) AS sort_trace_address
    FROM {{token_transfers}} tt
)
, finding_transfer AS (
    SELECT tt.to
    , MIN_BY(tt.unique_key, (tt.sort_block_number, tt.sort_tx_index, tt.sort_trace_address)) AS unique_key
    FROM token_transfers_with_sort_keys tt
    {% if is_incremental() %}
    LEFT JOIN {{this}} t
        ON t.address=tt.to
    WHERE t.address IS NULL
    AND {{ incremental_predicate('tt.block_time') }}
    {% endif %}
    GROUP BY 1
)

SELECT '{{blockchain}}' as blockchain
, tt.to AS address
, tt."from" AS first_receive_from
, tt.tx_from AS first_receive_executed_by
, tt.amount AS amount
, tt.amount_usd AS amount_usd
, tt.token_standard AS token_standard
, tt.contract_address AS token_address
, tt.block_time AS block_time
, tt.block_number AS block_number
, tt.tx_hash AS tx_hash
, tt.tx_index AS tx_index
, tt.trace_address AS trace_address
, tt.block_month AS block_month
, unique_key
FROM {{token_transfers}} tt
INNER JOIN finding_transfer ft USING (unique_key)
{% if is_incremental() %}
WHERE {{ incremental_predicate('tt.block_time') }}
{% endif %}
{% endmacro %}