{% macro addresses_events_first_token_received(blockchain, token_transfers) %}

WITH filtered_transfers AS (
    SELECT tt.*
    FROM {{token_transfers}} tt
    {% if is_incremental() %}
    WHERE tt.to NOT IN (SELECT address FROM {{this}})
    AND {{ incremental_predicate('tt.block_time') }}
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
                    ARRAY[COALESCE(CAST(tt.evt_index AS bigint), -1)]
                ),
                x -> COALESCE(x, -1)
            )
    ) AS rn
    FROM filtered_transfers tt
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
, tt.unique_key AS unique_key
FROM ranked_transfers tt
WHERE tt.rn = 1
{% endmacro %}