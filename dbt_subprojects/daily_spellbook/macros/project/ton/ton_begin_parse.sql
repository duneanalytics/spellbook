{%- macro ton_begin_parse()
-%}
ARRAY[{{ ton_action_begin_parse()}}, null]
{%- endmacro -%}