{%- macro ton_return_if_neq(value, expected)
-%}
ARRAY[{{ ton_action_return_if_neq() }}, null, null, '{{ value }}', '{{ expected }}']
{%- endmacro -%}