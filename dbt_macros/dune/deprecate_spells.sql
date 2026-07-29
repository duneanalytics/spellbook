{% macro deprecate_spells() %}
  {%- if target.name == 'prod' -%}
    {%- set properties = {
            'dune.created_by': 'dbt_spellbook',
            'dune.public': 'false',
            'dune.visible': 'false',
            'dune.data_explorer.category': 'abstraction',
            'dune.vacuum': '{"enabled":true}'
          } -%}
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
