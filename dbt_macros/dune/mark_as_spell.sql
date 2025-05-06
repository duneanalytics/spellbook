{% macro mark_as_spell(this, materialization) %}
  {%- if target.name == 'prod' -%}
    {%- if model.config.materialized == "view" -%}
      {%- set properties = { 'dune.data_explorer.category': 'abstraction' } -%}
      CALL {{ model.database }}._internal.alter_view_properties('{{ model.schema }}', '{{ model.alias }}',
        {{ trino_properties(properties) }}
      )
    {%- else -%}
      {%- set properties = { 'dune.data_explorer.category': 'abstraction', 'dune.vacuum': '{"enabled":true}' } -%}
      ALTER TABLE {{ this }}
      SET PROPERTIES extra_properties = {{ trino_properties(properties) }}
    {%- endif -%}
  {%- endif -%}
{%- endmacro -%}
