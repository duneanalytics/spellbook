{% macro create_schema(relation) -%}
  {{ adapter.dispatch('create_schema', 'dbt')(relation) }}
{% endmacro %}

{% macro default__create_schema(relation) -%}
  {% set s3_bucket = var('DBT_ENV_CUSTOM_ENV_S3_BUCKET', 'local') %}
  {%- if target.name == 'dev' -%}
    {%- set catalog = 'hive/' -%}
  {%- endif -%}
  {%- call statement('create_schema') -%}
   CREATE SCHEMA {{ relation }} WITH (location = 's3a://{{s3_bucket}}/{catalog}')
  {% endcall %}
{% endmacro %}
