
{% macro properties(properties, relation) %}
  {% do log(relation, info=true) %}

  {% set s3_bucket = var('DBT_ENV_CUSTOM_ENV_S3_BUCKET', 'local') %}
  {%- if properties is not none -%}
      WITH (
          location = 's3a://{{s3_bucket}}/hive/{{relation.schema}}/{{relation.identifier | replace("__dbt_tmp","")}}'
          {%- for key, value in properties.items() -%}
            {{ key }} = {{ value }}
            {%- if not loop.last -%}{{ ',\n  ' }}{%- endif -%}
          {%- endfor -%}
      )
  {%- else -%}
      WITH (
          location = 's3a://{{s3_bucket}}/hive/{{relation.schema}}/{{relation.identifier | replace("__dbt_tmp","")}}'
      )
  {%- endif -%}
{%- endmacro -%}


{% macro trino__create_table_as(temporary, relation, sql) -%}
  {%- set _properties = config.get('properties') -%}
  create table {{ relation }}
    {{ properties(_properties, relation) }}
  as (
    {{ sql }}
  );
{% endmacro %}