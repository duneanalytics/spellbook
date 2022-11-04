
{% macro databricks__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {% set s3_bucket = var('DBT_ENV_CUSTOM_ENV_S3_BUCKET', 'local') %}

  {%- if language == 'sql' -%}
    {%- if temporary -%}
      {{ create_temporary_view(relation, compiled_code) }}
    {%- else -%}
      {% if config.get('file_format', default='delta') == 'delta' %}
        create or replace table {{ relation }}
      {% else %}
        create table {{ relation }}
      {% endif %}
            {% if s3_bucket != 'local' %}
                {{ file_format_clause() }} location "{{ 's3a://'+ s3_bucket + '/' +  relation | replace(".","/") }}"
             {% else %}
                {{ file_format_clause() }}
            {% endif %}
      {{ options_clause() }}
      {{ partition_cols(label="partitioned by") }}
      {{ clustered_cols(label="clustered by") }}
      {{ location_clause() }}
      {{ comment_clause() }}
      {{ tblproperties_clause() }}
      as
      {{ compiled_code }}
    {%- endif -%}
  {%- elif language == 'python' -%}
    {#--
    N.B. Python models _can_ write to temp views HOWEVER they use a different session
    and have already expired by the time they need to be used (I.E. in merges for incremental models)

    TODO: Deep dive into spark sessions to see if we can reuse a single session for an entire
    dbt invocation.
     --#}
    {{ py_write_table(compiled_code=compiled_code, target_relation=relation) }}
  {%- endif -%}
{%- endmacro -%}

{%- macro spark__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {% set s3_bucket = var('DBT_ENV_CUSTOM_ENV_S3_BUCKET', 'local') %}
  {%- if language == 'sql' -%}
    {%- if temporary -%}
      {{ create_temporary_view(relation, compiled_code) }}
    {%- else -%}
      {% if config.get('file_format', validator=validation.any[basestring]) == 'delta' %}
        create or replace table {{ relation }}
      {% else %}
        create table {{ relation }}
      {% endif %}
        {% if s3_bucket != 'local' %}
            {{ file_format_clause() }} location "{{ 's3a://'+ s3_bucket + '/' +  relation | replace(".","/") }}"
         {% else %}
            {{ file_format_clause() }}
        {% endif %}
      {{ options_clause() }}
      {{ partition_cols(label="partitioned by") }}
      {{ clustered_cols(label="clustered by") }}
      {{ location_clause() }}
      {{ comment_clause() }}
      as
      {{ compiled_code }}
    {%- endif -%}
  {%- elif language == 'python' -%}
    {#--
    N.B. Python models _can_ write to temp views HOWEVER they use a different session
    and have already expired by the time they need to be used (I.E. in merges for incremental models)

    TODO: Deep dive into spark sessions to see if we can reuse a single session for an entire
    dbt invocation.
     --#}
    {{ py_write_table(compiled_code=compiled_code, target_relation=relation) }}
  {%- endif -%}
{%- endmacro -%}