{% macro trino__create_table_as(temporary, relation, sql) -%}
  {%- set _properties = config.get('properties') -%}
  create or replace table {{ relation }}
    {{ create_table_properties(_properties, relation) }}
  as (
    {{ sql }}
  );
{% endmacro %}

{% macro create_table_properties(properties, relation) %}
  {% set s3_bucket = var('DBT_ENV_CUSTOM_ENV_S3_BUCKET', 'local') %}
  {% set modified_identifier = relation.identifier | replace("__dbt_tmp", "") %}
  {%- set unique_location = modified_identifier ~ '_' ~ time_salted_md5_prefix() -%}
      WITH (
          location = 's3a://{{s3_bucket}}/hive/{{relation.schema}}/{{unique_location}}'
      )
{%- endmacro -%}

{% macro time_salted_md5_prefix(input_string=None) -%}
  {% if input_string is none -%}
    {% set input_string = modules.datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') %}
  {%- endif %}
  {{- local_md5(input_string) -}}
{%- endmacro %}
