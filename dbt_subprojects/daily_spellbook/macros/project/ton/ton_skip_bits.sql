{%- macro ton_skip_bits(offset)
-%}
ARRAY[{{ ton_action_skip_bits() }}, null, null, CAST({{ offset }} AS varchar)]

{%- endmacro -%}