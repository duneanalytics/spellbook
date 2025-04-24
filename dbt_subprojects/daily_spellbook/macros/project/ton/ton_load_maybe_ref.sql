{%- macro ton_load_maybe_ref()
-%}
ARRAY[{{ ton_action_load_maybe_ref() }}, null]
{%- endmacro -%}