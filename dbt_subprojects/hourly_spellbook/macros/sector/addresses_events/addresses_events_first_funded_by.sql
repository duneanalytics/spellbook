{% macro addresses_events_first_funded_by(blockchain, token_transfers) %}

WITH filtered_transfers AS (
    SELECT tt.*
    FROM {{token_transfers}} tt
    WHERE tt.token_standard = 'native'
    {% if is_incremental() %}
    AND {{ incremental_predicate('tt.block_time') }}
    AND tt.to NOT IN (SELECT address FROM {{this}})
    {% endif %}
)
, ranked_transfers AS (
    SELECT tt.*
    , ROW_NUMBER() OVER (
        PARTITION BY tt.to
        ORDER BY
            COALESCE(CAST(tt.block_number AS bigint), 9223372036854775807),
            COALESCE(CAST(tt.tx_index AS bigint), 9223372036854775807),
            TRANSFORM(
                COALESCE(
                    CAST(tt.trace_address AS array(bigint)),
                    CAST(ARRAY[] AS array(bigint))
                ),
                x -> COALESCE(x, -1)
            )
    ) AS rn
    FROM filtered_transfers tt
)

SELECT '{{blockchain}}' as blockchain
, tt.to AS address
, tt."from" AS first_funded_by
, tt.tx_from AS first_funding_executed_by
, tt.amount AS amount
, tt.amount_usd AS amount_usd
, tt.block_time AS block_time
, tt.block_number AS block_number
, tt.tx_hash AS tx_hash
, tt.tx_index AS tx_index
, tt.trace_address AS trace_address
, tt.unique_key AS unique_key
FROM ranked_transfers tt
WHERE tt.rn = 1

{% endmacro %}
