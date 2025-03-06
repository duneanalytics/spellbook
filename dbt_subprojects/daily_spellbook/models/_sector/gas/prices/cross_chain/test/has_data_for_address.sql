{% test has_data_for_address(model, column_name, address) %}
with model_sample as (
    select
        {{ column_name }}
    from {{ model }}
    where address = {{ address }} and address_prefix = CAST(SUBSTRING(LOWER(CAST(address AS VARCHAR)), 3, 2) AS VARCHAR)
)
select
    case when count(*) = 0 then 1 else 0 end as failed
from model_sample
having failed = 1
{% endtest %}