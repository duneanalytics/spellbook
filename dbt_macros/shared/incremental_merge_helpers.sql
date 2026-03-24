{% macro trino_safe_column_value(alias, col_name, col_type=none) %}
	{%- set col_ref = alias ~ '.' ~ adapter.quote(col_name) -%}
	{%- set is_array_type = col_type is not none and 'array' in (col_type | lower) -%}
	case
		when {{ col_ref }} is null then '__dbt_null__'
		else
			{%- if is_array_type %}
			{#- two-arg array_join skips null elements; use null_replacement so position/count of nulls affects the hash #}
			array_join({{ col_ref }}, ',', '__dbt_array_elem_null__')
			{%- else %}
			cast({{ col_ref }} as varchar)
			{%- endif %}
	end
{% endmacro %}


{% macro row_hash(alias, columns) %}
	{% if columns is none or columns | length == 0 %}
		md5(to_utf8('__no_compare__'))
	{% elif columns | length == 1 %}
		{%- set col = columns[0] -%}
		{%- set col_name = col if col is string else col.name -%}
		{%- set col_type = none if col is string else (col.data_type | default(none)) -%}
		md5(to_utf8(to_hex(md5(to_utf8({{ trino_safe_column_value(alias, col_name, col_type) }})))))
	{% else %}
		{#- per-column md5 avoids cross-column delimiter collisions (e.g. ('a','b|c') vs ('a|b','c')) #}
		md5(
			to_utf8(
				concat(
					{% for col in columns %}
						{%- set col_name = col if col is string else col.name -%}
						{%- set col_type = none if col is string else (col.data_type | default(none)) -%}
						to_hex(md5(to_utf8({{ trino_safe_column_value(alias, col_name, col_type) }})))
						{%- if not loop.last %}, '|', {% endif -%}
					{% endfor %}
				)
			)
		)
	{% endif %}
{% endmacro %}
