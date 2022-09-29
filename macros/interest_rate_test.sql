{% macro interest_rate_test(principal_df) %}
        {{ log('running macro') }}
        {% set final_values = [] %}

        {% for row in principal_df -%}
                {{ log(row) }}
                {{ final_values.append(row) }}
        {% endfor %}
        {{ return(final_values) }}
{%- endmacro -%}
