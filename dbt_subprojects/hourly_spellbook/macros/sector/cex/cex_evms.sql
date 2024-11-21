{% macro cex_evms(cex_addresses, blockchain, traces) %}

SELECT '{{blockchain}}' AS blockchain
, a.address
, a.cex_name
, a.distinct_name
, a.added_by
, a.added_date
, MIN(t.block_time) AS first_used
FROM {{traces}} t
INNER JOIN {{cex_addresses}} a ON a.address = t.to
{% if is_incremental() %}
LEFT JOIN {{this}} b ON a.address = b.address
    AND b.address IS NULL
WHERE {{incremental_predicate('t.block_time')}}
{% endif %}
GROUP BY a.address, a.cex_name, a.distinct_name, a.added_by, a.added_date

{% endmacro %}