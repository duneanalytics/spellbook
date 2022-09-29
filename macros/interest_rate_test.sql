{% macro interest_rate_test(principal_df) %}
        {{ log('running macro') }}
        {% set final_values = [] %}

        {% for row in new_events_list -%}
                {{ log(row) }}
                {{ final_values.append(row) }}
        {% endfor %}
        {{ return(final_values) }}
{%- endmacro -%}
