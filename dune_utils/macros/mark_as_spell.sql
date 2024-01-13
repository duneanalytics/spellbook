{% macro mark_as_spell(this, materialization) %}
  {%- if target.name == 'prod' -%}
    {%- set properties = { 'dune.data_explorer.category': 'abstraction' } -%}
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
