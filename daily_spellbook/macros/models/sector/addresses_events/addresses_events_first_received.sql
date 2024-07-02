{% macro addresses_events_first_received(blockchain, token_transfers) %}

WITH identify_first AS (
    SELECT tt.to AS address
    , MIN(tt.block_number) AS block_number
    , MIN_BY(tt.tx_index, (tt.block_number, tt.tx_index)) AS tx_index
    , MIN_BY(tt.evt_index, (tt.block_number, tt.tx_index, COALESCE(tt.evt_index, 0))) AS evt_index
    FROM {{token_transfers}} tt
    {% if is_incremental() %}
    LEFT JOIN {{this}} t ON t.address = tt.to
        AND t.blockchain IS NULL
    WHERE {{ incremental_predicate('tt.block_time') }}
    {% endif %}
    GROUP BY tt.to
    )

SELECT '{{blockchain}}' AS blockchain
, tt.to AS address
, tt.block_date
, tt.block_time
, tt.block_number
, tt.tx_hash
, tt.tx_index
, tt.evt_index
, tt.tx_from
, tt.tx_to
, tt.contract_address
, tt.trace_address
, tt.token_standard
, tt.symbol
, tt.amount_raw
, tt.amount
, tt.price_usd
, tt.amount_usd
, tt.unique_key
, tt."from"
FROM {{token_transfers}} tt
INNER JOIN identify_first iff ON tt.block_number=iff.block_number
    AND tt.to=iff.address
    AND tt.tx_index = iff.tx_index
    AND tt.evt_index = iff.evt_index
{% if is_incremental() %}
WHERE {{ incremental_predicate('tt.block_time') }}
{% endif %}

{% endmacro %}