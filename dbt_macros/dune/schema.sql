{% macro create_schema(relation) -%}
  {{ adapter.dispatch('create_schema', 'dbt')(relation) }}
{% endmacro %}

{% macro trino__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    {%- if target.database == 'dune' -%}
      CREATE SCHEMA IF NOT EXISTS {{ relation }}
    {%- else -%}
      CREATE SCHEMA {{ relation }} WITH (location = 's3a://{{s3_bucket()}}/')
    {%- endif -%}
  {% endcall %}
{% endmacro %}
