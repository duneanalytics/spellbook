{% macro transfers_base_full_refresh_time_filter(time_column) -%}
{{ time_column }} >= current_date - interval '7' day
{%- endmacro %}
