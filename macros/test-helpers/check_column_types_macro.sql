{% macro check_column_types_macro(model, column_types) %}
with test_sample as (
select * from {{model}} limit 1
)
, equality_checks as (
  {%- for col, col_type in column_types.items() %}
  select '{{col}}' column_name, '{{col_type}}' as expected_type, typeof({{col}}) as actual_type
  from test_sample
  {% if not loop.last %}union all{% endif %}
  {% endfor -%}
)
select * from equality_checks where not contains(split(expected_type,'/'),actual_type)
{% endmacro %}
