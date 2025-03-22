{%- macro ton_load_int(size, as)
-%}
{% if size > 128 %}
  {{ exceptions.raise_compiler_error("size for ton_load_int must be less than 128") }}
{% endif %}
ARRAY[{{ ton_action_load_int() }}, '{{ as }}', {% if size < 64 %} 'bigint' {% else %} 'INT256' {% endif %}, '{{ size }}']

{%- endmacro -%}