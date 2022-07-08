{% macro bytea2numeric(column_name) %}
   conv(({{ column_name }}), 16,10)
{% endmacro %}
