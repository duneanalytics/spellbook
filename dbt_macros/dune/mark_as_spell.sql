{% macro mark_as_spell(this, materialization) %}
  {%- if target.name == 'prod' -%}
    {%- if model.config.materialized == "view" -%}
      {%- set properties = { 'dune.data_explorer.category': 'abstraction', 'dune.data_explorer.freshness': var('freshness') } -%}
    {%- else -%}
      {%- set properties = { 'dune.data_explorer.category': 'abstraction', 'dune.data_explorer.freshness': var('freshness'), 'dune.vacuum': '{"enabled":true}' } -%}
    {%- endif -%}
    {%- set deprecated_at = model.config.get('deprecated_at', none) -%}
    {%- if deprecated_at -%}
      {%- do properties.update({'dune.data_explorer.deprecated_at': deprecated_at}) -%}
    {%- endif -%}
    {%- if model.config.materialized == "view" -%}
      CALL {{ model.database }}._internal.alter_view_properties('{{ model.schema }}', '{{ model.alias }}',
        {{ trino_properties(properties) }}
      )
    {%- else -%}
      ALTER TABLE {{ this }}
      SET PROPERTIES extra_properties = {{ trino_properties(properties) }}
    {%- endif -%}
  {%- endif -%}
{%- endmacro -%}
