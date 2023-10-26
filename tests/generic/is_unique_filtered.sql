{% test is_unique_filtered(model, column_name) %}

select
    {{ column_name }} as unique_field,
    count(*) as n_records

from {{ model }}
where {{ column_name }} is not null
    and block_date >= NOW() - interval '2' day
group by {{ column_name }}
having count(*) > 1

{% endtest %}
