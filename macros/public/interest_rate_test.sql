
{% macro interest_rate_test(principal, interest_amount, interest_rate, time_years=5) %}
        {% set final_values = [] %}

        {% for t in range(time_years) -%}
                {{ t, principal * (1 + interest_rate*t)}}
        {% endfor %}
{%- endmacro -%}
