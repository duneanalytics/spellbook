{% macro cex_evms(cex_addresses, blockchain, traces) %}

WITH new_addresses AS (
    SELECT 
        '{{blockchain}}' AS blockchain,
        a.address,
        a.cex_name,
        a.distinct_name,
        a.added_by,
        a.added_date,
        MIN(t.block_time) AS first_used
    FROM {{traces}} t
    INNER JOIN {{cex_addresses}} a ON a.address = t.to
    {% if is_incremental() %}
    WHERE {{incremental_predicate('t.block_time')}}
    {% endif %}
    GROUP BY a.address, a.cex_name, a.distinct_name, a.added_by, a.added_date
)

SELECT 
    n.blockchain,
    n.address,
    n.cex_name,
    n.distinct_name,
    n.added_by,
    n.added_date,
    {% if is_incremental() %}
    COALESCE(b.first_used, n.first_used) AS first_used
    FROM new_addresses n
    LEFT JOIN {{this}} b ON 
        n.blockchain = b.blockchain AND 
        n.address = b.address
    {% else %}
    n.first_used
    FROM new_addresses n
    {% endif %}

{% endmacro %}