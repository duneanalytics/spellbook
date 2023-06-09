{% test assert_type_decimal_38_0(model, column_name) %}
    with all_values as (
        select {{column_name}}
        from {{ model }}
        union all
        select cast (0 as decimal (38, 0)) as {{column_name}}
    )
    select {{column_name}} from all_values limit 0
{% endtest %}