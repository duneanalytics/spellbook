{% test unique_or_null(model, column_name) %}

select {{ column_name }} AS unique_field
, COUNT(*) AS n_records
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
GROUP BY {{ column_name }}
HAVING COUNT(*) > 1

{% endtest %}
