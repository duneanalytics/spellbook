{% macro expose_dataset(blockchains, contributors) %}
  {%- if target.name == 'prod' -%}
    {%- set properties = {
            'dune.created_by': 'dbt_spellbook',
            'dune.public': 'true',
            'dune.visible': 'true',
            'dune.data_explorer.blockchains':  blockchains | as_text,
            'dune.data_explorer.category': 'third_party_data',
            'dune.data_explorer.contributors': contributors | as_text,
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
