{% macro addresses_events_first_received(blockchain, token_transfers) %}

WITH identify_first AS (
    SELECT to AS address
    , MIN(block_number) AS block_number
    , MIN_BY(tx_index, (block_number, tx_index)) AS tx_index
    , MIN_BY(evt_index, (block_number, tx_index, evt_index)) AS evt_index
    FROM {{token_transfers}} tt
    {% if is_incremental() %}
    LEFT JOIN {{this}} t ON t.address = tt.to
        AND COALESCE(t.unique_key, 'NULL') = 'NULL'
    WHERE {{ incremental_predicate('tt.block_time') }}
    {% endif %}
    GROUP BY to
    )

SELECT '{{blockchain}}' AS blockchain
, to AS address
, block_date
, block_time
, block_number
, tx_hash
, tx_index
, evt_index
, tx_from
, tx_to
, contract_address
, trace_address
, token_standard
, symbol
, amount_raw
, amount
, price_usd
, amount_usd
, unique_key
, "from"
FROM {{token_transfers}} tt
INNER JOIN identify_first iff 
    ON COALESCE(tt.block_number, -1) = COALESCE(iff.block_number, -1)
    AND COALESCE(tt.tx_index, -1) = COALESCE(iff.tx_index, -1)
    AND COALESCE(tt.evt_index, -1) = COALESCE(iff.evt_index, -1)
{% if is_incremental() %}
WHERE {{ incremental_predicate('block_time') }}
{% endif %}

{% endmacro %}