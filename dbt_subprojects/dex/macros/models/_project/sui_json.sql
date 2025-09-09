{% macro j_str(col, path) -%}
json_extract_scalar({{ col }}, '{{ path }}')
{%- endmacro %}

{% macro j_num(col, path) -%}
try_cast(json_extract_scalar({{ col }}, '{{ path }}') as decimal(38,0))
{%- endmacro %}

{% macro j_dbl(col, path) -%}
try_cast(json_extract_scalar({{ col }}, '{{ path }}') as double)
{%- endmacro %}

{% macro j_bool(col, path) -%}
try_cast(json_extract_scalar({{ col }}, '{{ path }}') as boolean)
{%- endmacro %}
