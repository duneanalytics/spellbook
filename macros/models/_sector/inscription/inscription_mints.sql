{% macro inscription_mints(blockchain, transactions, first_inscription_block) %}

WITH raw_inscriptions AS (
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
    AND block_number >= {{first_inscription_block}}
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
, json_extract_scalar(data_filtered, '$.p') AS inscription_standard
, json_extract_scalar(data_filtered, '$.op') AS operation
, json_extract_scalar(data_filtered, '$.tick') AS inscription_symbol
, try_cast(json_extract_scalar(data_filtered, '$.amt') AS UINT256) AS amount
, REGEXP_EXTRACT(data_filtered, '"vin":(\\[.*?\\])') AS vin
, REGEXP_EXTRACT(data_filtered, '"vout":(\\[.*?\\])') AS vout
, data_filtered AS full_inscription
FROM raw_inscriptions

{% endmacro %}