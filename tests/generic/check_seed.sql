-- this tests checks a model for every row in a seed file.
-- you need to specify the matching columns and the columns to check for equality.

{% test check_seed(model, seed_file, match_columns=[], check_columns=[]) %}
    {# --jinja comment
--    I wish we could do something dynamic like this below, but it is prohibited by having a local database connection.. :(
--    {%- set unique_columns = config.get('unique_key') -%}
--    {%- set seed_check_columns = dbt_utils.get_filtered_columns_in_relation(from=seed_file, except=unique_columns) -%}
--    {%- set seed_matching_columns = dbt_utils.get_filtered_columns_in_relation(from=seed_file, except=seed_check_columns) -%}
    --jinja comment #}
    {{ config(severity = 'warn') }}
    {%- set seed_check_columns = check_columns -%}
    {%- set seed_matching_columns = match_columns -%}

    -- check if the matching columns return singular results
    with matching_count_test as (
        select
        'matched records count' as test_description,
        count(model.{{seed_matching_columns[0]}}) as result,
        1 as expected,
        {%- for column_name in seed_matching_columns %}
        seed.{{column_name}} {% if not loop.last %},{% endif %}
        {% endfor -%}
        from {{seed_file}} seed
        left join {{model}} model
        ON 1=1
        {%- for column_name in seed_matching_columns %}
        AND seed.{{column_name}} = model.{{column_name}}
        {% endfor -%}
        GROUP BY
        {%- for column_name in seed_matching_columns %}
        seed.{{column_name}} {% if not loop.last %},{% endif %}
        {% endfor -%}
    ) ,

    equality_tests as
    (
        {%- for checked_column in seed_check_columns %}
        select
        'equality test: {{checked_column}}' as test_description
        ,test.*
        from (
            select
            model.{{checked_column}} as result,
            seed.{{checked_column}} as expected,
            {%- for column_name in seed_matching_columns %}
            seed.{{column_name}} {% if not loop.last %},{% endif %}
            {% endfor -%}

            from {{seed_file}} seed
            left join {{model}} model
            ON 1=1
            {%- for column_name in seed_matching_columns %}
            AND seed.{{column_name}} = model.{{column_name}}
            {% endfor -%}
        ) test
        {%- if not loop.last %}
        UNION ALL
        {% endif -%}
        {% endfor -%}

    )


    select * from (
        select *
        from matching_count_test
        union all
        select *
        from equality_tests
    ) all
    where result != expected
{% endtest %}
