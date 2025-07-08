{% macro addresses_events_first_token_received(blockchain, token_transfers) %}

WITH finding_transfer AS (
    SELECT tt.to
    , MIN_BY(tt.unique_key, (tt.block_number, tt.tx_index, COALESCE(tt.trace_address, ARRAY[COALESCE(evt_index, -1)]))) AS unique_key
    FROM {{token_transfers}} tt
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