-- This macro returns no rows if it finds records in the given column using the where clause.
{% test has_entries(model, column_name, where) %}
with model_sample as (
    select
        {{ column_name }}
    from {{ model }}
    where {{ where }}
)
select
    case when count(*) = 0 then 1 else 0 end as failed
from model_sample
having failed = 1
{% endtest %}