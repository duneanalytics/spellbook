
-- This macro returns no rows if it finds records in the given column using the where clause.
{% macro has_entries(model, column_name, where) %}
with model_sample as (
    select
        {{ column_name }}
    from {{ model }}
    where {{ where }}
)
select
    count(*) as count
from model_sample
where count = 0
{% endmacro %}