-- this tests checks the column types of a model
{% test check_column_types(model, column_types) %}
    {{ check_column_types_macro(model,column_types) }}
{% endtest %}
