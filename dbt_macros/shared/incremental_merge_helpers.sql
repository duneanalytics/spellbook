{% macro trino_safe_column_value(alias, col_name, col_type=none) %}
	{%- set col_ref = alias ~ '.' ~ adapter.quote(col_name) -%}
	{%- set is_array_type = col_type is not none and 'array' in (col_type | lower) -%}
	case
		when {{ col_ref }} is null then '__dbt_null__'
		else
			{%- if is_array_type %}
			array_join({{ col_ref }}, ',')
			{%- else %}
			cast({{ col_ref }} as varchar)
			{%- endif %}
	end
{% endmacro %}


{% macro row_hash(alias, columns) %}
	{% if columns is none or columns | length == 0 %}
		md5(to_utf8('__no_compare__'))
	{% else %}
		md5(
			to_utf8(
				concat(
					{% for col in columns %}
						{%- set col_name = col if col is string else col.name -%}
						{%- set col_type = none if col is string else (col.data_type | default(none)) -%}
						{{ trino_safe_column_value(alias, col_name, col_type) }}
						{%- if not loop.last %}, '|', {% endif -%}
					{% endfor %}
				)
			)
		)
	{% endif %}
{% endmacro %}
