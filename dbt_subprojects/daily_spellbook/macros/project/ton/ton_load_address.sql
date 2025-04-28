{%- macro ton_load_address(as, raise_on_error=true)
-%}
ARRAY[{{ ton_action_load_address() }}, '{{ as }}', 'varchar', '{{ raise_on_error }}']
{%- endmacro -%}