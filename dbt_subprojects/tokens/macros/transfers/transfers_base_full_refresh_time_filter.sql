{% macro transfers_base_full_refresh_time_filter(time_column) -%}
{{ time_column }} >= current_date - interval '7' day
{%- endmacro %}

{% macro transfers_full_refresh_time_filter(time_column, transfers_start_date=none) -%}
{# When true, non-incremental full refresh uses transfers_start_date (full history) instead of transfers_base_full_refresh_time_filter. #}
{%- set transfers_enrich_full_refresh_uses_start_date = false -%}
{%- if transfers_enrich_full_refresh_uses_start_date
	and transfers_start_date is not none
	and transfers_start_date | trim != '' -%}
{{ time_column }} >= timestamp '{{ transfers_start_date }}'
{%- else -%}
{{ transfers_base_full_refresh_time_filter(time_column) }}
{%- endif -%}
{%- endmacro %}
