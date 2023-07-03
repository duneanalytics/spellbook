{% macro trino__create_table_as(temporary, relation, sql) -%}
  {%- set _properties = config.get('properties') -%}
  create or replace table {{ relation }}
    {{ create_table_properties(_properties, relation) }}
  as (
    {{ sql }}
  );
{% endmacro %}

{% macro create_table_properties(properties, relation) %}
  {% set modified_identifier = relation.identifier | replace("__dbt_tmp", "") %}
  {%- set unique_location = modified_identifier ~ '_' ~ time_salted_md5_prefix() -%}
      WITH (
          location = 's3a://{{s3_bucket()}}/{{relation.schema}}/{{unique_location}}'
      )
{%- endmacro -%}

{% macro time_salted_md5_prefix(input_string=None) -%}
  {% if input_string is none -%}
    {% set input_string = modules.datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') %}
  {%- endif %}
  {{- local_md5(input_string) -}}
{%- endmacro %}

{%- macro s3_bucket() -%}
  {%- if target.type == 'trino' and target.schema != 'wizard' -%}
    {%- if target.name == 'prod' or target.schema.startswith('git_dunesql') -%}
      {{- return('prod-spellbook-trino-118330671040') -}}
    {%- else -%}
      {{- return('trino-dev-datasets-118330671040') }}
    {%- endif -%}
  {%- else -%}
    {{- return(var('DBT_ENV_CUSTOM_ENV_S3_BUCKET', 'local')) -}}
  {%- endif -%}
{%- endmacro -%}