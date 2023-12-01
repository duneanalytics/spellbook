{#
  This macro is used in https://github.com/starburstdata/dbt-trino/blob/master/dbt/include/trino/macros/materializations/seeds/helpers.sql
  We need to override the type bindings to support varbinary hex addresses and hashes in seeds.
#}


{% macro create_bindings(row, types) %}
  {% set values = [] %}
  {% set re = modules.re %}

  {%- for item in row -%}
      {%- set type = types[loop.index0] -%}
      {%- set match_type = re.match("(\w+)(\(.*\))?", type) -%}
      {%- if item is not none and 'varbinary' in type.lower() -%}
        {%- do values.append((none, item )) -%}
      {%- elif item is not none and item is string and 'interval' in match_type.group(1) -%}
        {%- do values.append((none, match_type.group(1).upper() ~ " " ~ item)) -%}
      {%- elif item is not none and item is string and 'varchar' not in type.lower() -%}
        {%- do values.append((none, match_type.group(1).upper() ~ " '" ~ item ~ "'")) -%}
      {%- elif item is not none and 'varchar' in type.lower() -%}
        {%- do values.append((get_binding_char(), item|string())) -%}
      {%- else -%}
        {%- do values.append((get_binding_char(), item)) -%}
      {% endif -%}
  {%- endfor -%}
  {{ return(values) }}
{% endmacro %}
