{% macro inscription_all(blockchain, transactions, first_inscription_block) %}

SELECT '{{blockchain}}' AS blockchain
, block_time
, date(date_trunc('month', block_time)) AS block_month
, block_number
, hash AS tx_hash
, index AS tx_index
, "from" AS tx_from
, to AS tx_to
, substring(from_utf8(data), position('{' IN from_utf8(data))) AS full_inscription
FROM {{transactions}}
WHERE ("LEFT"(from_utf8(data), 8)='data:,{"') = TRUE
AND success
AND block_number >= {{first_inscription_block}}
{% if is_incremental() %}
AND {{ incremental_predicate('block_time') }}
{% endif %}

{% endmacro %}
