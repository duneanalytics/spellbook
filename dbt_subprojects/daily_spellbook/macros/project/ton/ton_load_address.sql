{%- macro ton_load_address(as)
-%}
ARRAY[{{ ton_action_load_address() }}, '{{ as }}', 'varchar']
{%- endmacro -%}