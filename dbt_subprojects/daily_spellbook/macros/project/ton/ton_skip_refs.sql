{%- macro ton_skip_refs(offset)
-%}
ARRAY[{{ ton_action_skip_ref() }}, null, null, CAST({{ offset }} AS varchar)]

{%- endmacro -%}