{% macro create_schema(relation) -%}
  {{ adapter.dispatch('create_schema', 'dbt')(relation) }}
{% endmacro %}

{% macro default__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
   CREATE SCHEMA {{ relation }} WITH (location = 's3a://{{s3_bucket()}}/')
  {% endcall %}
{% endmacro %}
