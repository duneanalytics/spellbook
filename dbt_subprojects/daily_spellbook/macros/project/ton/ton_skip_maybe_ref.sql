{%- macro ton_skip_maybe_ref()
-%}
ARRAY[{{ ton_action_skip_maybe_ref() }}, null]

{%- endmacro -%}