{% test is_unique_filtered(model, column_name) %}

select
    {{ column_name }} as unique_field,
    count(*) as n_records

from {{ model }}
where {{ column_name }} is not null
and block_date >= (select (coalesce(max(block_date), '1900-01-01 00:00') - interval 2 days) from {{ model }})
group by {{ column_name }}
having count(*) > 1

{% endtest %}