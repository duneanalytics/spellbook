{% macro cex_evms(cex_addresses, blockchain, traces) %}

SELECT '{{blockchain}}' AS blockchain
, address
, cex_name
, distinct_name
, added_by
, added_date
, MIN(t.block_time) AS first_used
FROM {{traces}} t
INNER JOIN {{addresses}} a ON a.address = t.to
{% if is_incremental() %}
LEFT JOIN {{this}} b ON a.address = b.address
    AND b.address IS NULL
WHERE {{incremental_predicate('t.block_time')}}
{% endif %}

{% endmacro %}