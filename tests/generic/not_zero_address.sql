{% test not_zero_address(model, column_name) %}
select
*
from {{ model }}
where {{column_name }} = 0x0000000000000000000000000000000000000000
{% endtest %}
