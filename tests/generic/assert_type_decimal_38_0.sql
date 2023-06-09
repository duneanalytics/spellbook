{% test assert_type_decimal_38_0(model, column_name) %}
    with all_values as (
        select {{ column_name }}
        from {{ model }}
    )
    , union_all_test as (
        select {{ column_name }} from all_values
        union all
        select cast (0 as decimal (38, 0)) as {{ column_name }}
    )
    , arithmetic_test as (
        select {{ column_name }}
        from union_all_test
        where  {{ column_name }} + cast (0 as decimal (38, 0)) <> cast(({{ column_name }} + 0) as decimal (38, 0))
            or {{ column_name }} + cast (0 as decimal (38, 0)) <=  {{ column_name }} + cast (-1 as decimal (38, 0))
            or {{ column_name }} + cast (0 as decimal (38, 0)) is null
    )
    select {{ column_name }} from arithmetic_test limit 100
{% endtest %}