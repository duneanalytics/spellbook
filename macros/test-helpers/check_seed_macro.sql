-- this macro is used in generic tests that check a model for every row in a seed file.
-- you need to specify the matching columns and the columns to check for equality.
-- filter: dictionary filter of column:value that is applied to the seed file

{% macro check_seed_macro(model, seed_file, seed_matching_columns=[], seed_check_columns=[], filter=None) %}

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
                    {% if column_name == 'trace_address' %}
                    AND COALESCE(CAST(split(seed.{{column_name}}, ',') as array<bigint>), ARRAY()) = model.{{column_name}}
                    {% else %}
                    AND seed.{{column_name}} = model.{{column_name}}
                    {% endif %}
                    {% endfor -%}
            ) model_sample
        ON 1=1
            {%- for column_name in seed_matching_columns %}
            {% if column_name == 'trace_address' %}
            AND COALESCE(CAST(split(seed.{{column_name}}, ',') as array<bigint>), ARRAY()) = model_sample.{{column_name}}
            {% else %}
            AND seed.{{column_name}} = model_sample.{{column_name}}
            {% endif %}
            {% endfor -%}
        WHERE 1=1
              {%- if filter is not none %}
                  {%- for col, val in filter.items() %}
                      {% if val is not none %} AND seed.{{col}} = '{{val}}' {% endif %}
                  {% endfor -%}
              {% endif -%}
    ),

    -- check if the matching columns return singular results
    matching_count_test as (
        select
            'matched records count' as test_description,
            count(model_{{seed_matching_columns[0]}}) as `result_model`,
            1 as `expected_seed`,
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
                model_{{checked_column}} as `result_model`,
                seed_{{checked_column}} as `expected_seed`,
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
    where `result_model` != `expected_seed`
{% endmacro %}
