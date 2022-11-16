{% test nft_trades_column_values_matching(model, seed_file, match_columns=[], check_columns=[]) %}

    -- test idea from pr/1707
    {% set seed_check_columns = check_columns %}
    {% set seed_matching_columns = match_columns %}

    with matched_records as (
        select
        {%- for column_name in seed_matching_columns %}
        seed.{{column_name}} as seed_{{column_name}},
        model_sample.{{column_name}} as model_{{column_name}},
        {% endfor -%}
        {%- for column_name in seed_check_columns %}
        seed.{{column_name}} as seed_{{column_name}},
        model_sample.{{column_name}} as model_{{column_name}} {% if not loop.last %},{% endif %}
        {% endfor -%}
        from {{seed_file}} seed
        left join (
            select
            {%- for column_name in seed_matching_columns %}
            model.{{column_name}},
            {% endfor -%}
            {%- for column_name in seed_check_columns %}
            model.{{column_name}} {% if not loop.last %},{% endif %}
            {% endfor -%}
            from  {{seed_file}} seed
            inner join {{model}} model
                ON 1=1
                {%- for column_name in seed_matching_columns %}
                AND seed.{{column_name}} = model.{{column_name}}
                {% endfor -%}
            ) model_sample
        ON 1=1
        {%- for column_name in seed_matching_columns %}
        AND seed.{{column_name}} = model_sample.{{column_name}}
        {% endfor -%}
    ),

    -- check if the matching columns return singular results
    matching_count_test as (
        select
        'matched records count' as test_description,
        count(model_{{seed_matching_columns[0]}}) as `result (model)`,
        1 as `expected (seed)`,
        {%- for column_name in seed_matching_columns %}
        seed_{{column_name}} as {{column_name}}{% if not loop.last %},{% endif %}
        {% endfor -%}
        from matched_records
        GROUP BY
        {%- for column_name in seed_matching_columns %}
        seed_{{column_name}} {% if not loop.last %},{% endif %}
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
            model_{{checked_column}} as `result (model)`,
            seed_{{checked_column}} as `expected (seed)`,
            {%- for column_name in seed_matching_columns %}
            seed_{{column_name}} {% if not loop.last %},{% endif %}
            {% endfor -%}
            from matched_records
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
    where `result (model)` != `expected (seed)`

{% endtest %}