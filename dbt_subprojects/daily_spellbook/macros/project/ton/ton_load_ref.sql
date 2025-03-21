{%- macro ton_load_ref()
-%}
ARRAY[{{ ton_action_load_ref() }}, null]
{%- endmacro -%}