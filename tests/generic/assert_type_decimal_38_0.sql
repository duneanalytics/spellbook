{% test assert_type_decimal_38_0(model, column_name) %}
    with arithmetic_test as (
        select {{column_name}}
        from {{ model }}
        where  {{column_name}} + cast (0 as decimal (38, 0)) <> cast(({{column_name}} + 0) as decimal (38, 0))
            or {{column_name}} + cast (0 as decimal (38, 0)) <=  {{column_name}} + cast (-1 as decimal (38, 0))
            or {{column_name}} + cast (0 as decimal (38, 0)) is null
    )
    select {{column_name}} from arithmetic_test limit 100
{% endtest %}