{%- macro ton_load_coins(as)
-%}
ARRAY[{{ ton_action_load_coins() }}, '{{ as }}', 'UINT256']
{%- endmacro -%}