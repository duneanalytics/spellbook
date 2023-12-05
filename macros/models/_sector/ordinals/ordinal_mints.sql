{% macro ordinal_mints(blockchain, transactions, first_ordinal_block) %}

WITH raw_ordinals AS (
    SELECT block_time
    , block_number
    , hash AS tx_hash
    , index AS tx_index
    , "from" AS tx_from
    , to AS tx_to
    , substring(from_utf8(data), position('{' IN from_utf8(data))) AS data_filtered
    FROM {{transactions}}
    WHERE ("LEFT"(from_utf8(data), 8)='data:,{"') = TRUE
    AND success
    AND block_number >= {{first_ordinal_block}}
    {% if is_incremental() %}
    AND {{ incremental_predicate('block_time') }}
    {% endif %}
    )

SELECT '{{blockchain}}' AS blockchain
, block_time
, date_trunc('month', block_time) AS block_month
, block_number
, tx_hash
, tx_from
, tx_to
, tx_index
, json_extract_scalar(data_filtered, '$.p') AS ordinal_standard
, json_extract_scalar(data_filtered, '$.op') AS operation
, json_extract_scalar(data_filtered, '$.tick') AS ordinal_symbol
, try_cast(json_extract_scalar(data_filtered, '$.amt') AS UINT256) AS amount
, JSON_EXTRACT(data_filtered, '$.vin') AS vin
, JSON_EXTRACT(data_filtered, '$.vout') AS vout
, data_filtered AS full_inscription
FROM raw_ordinals

{% endmacro %}