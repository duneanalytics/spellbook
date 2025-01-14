{% macro test_reasonable_usd_amount(model, column_name, max_value=1000000000) %}

{# 
    Test to ensure USD amounts are within a reasonable range
    Args:
        model: The model to test
        column_name: The column containing USD amounts
        max_value: Maximum allowed USD amount (default: $1B)
#}

WITH validation AS (
    SELECT
        {{ column_name }} as amount_usd,
        blockchain,
        project,
        version,
        tx_hash,
        evt_index,
        block_time
    FROM {{ model }}
    WHERE {{ column_name }} > {{ max_value }}
)

SELECT *
FROM validation
WHERE amount_usd is not null

{% endmacro %}
