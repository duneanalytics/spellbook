{%- macro ton_load_uint(size, as=false)
-%}
{% if size > 256 %}
  {{ exceptions.raise_compiler_error("size for ton_load_uint must be less than 256") }}
{% endif %}
ARRAY[{% if size == 256 %}{{ ton_action_load_uint_large() }}{% else %}{{ ton_action_load_uint() }}{% endif %}, '{{ as }}', {% if size > 63 %} 'UINT256' {% else %} 'bigint' {% endif %}, '{{ size }}']

{%- endmacro -%}