{% macro private_data_explorer(blockchains, spell_type, spell_name) %}
  {%- if target.name == 'prod' -%}
    {%- set properties = {
            'dune.created_by': 'dbt_spellbook',
            'dune.public': 'false',
            'dune.visible': 'true',
            'dune.data_explorer.blockchains':  blockchains | as_text,
            'dune.data_explorer.category': 'abstraction',
            'dune.data_explorer.abstraction.type': spell_type,
            'dune.data_explorer.abstraction.name': spell_name,
            'dune.data_explorer.freshness': var('freshness'),
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
