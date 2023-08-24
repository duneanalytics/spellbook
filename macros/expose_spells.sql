{% macro expose_spells(blockchains, spell_type, spell_name, contributors) %}
  {%- if target.name == 'prod' -%}
    {%- if 'dunesql' not in model.config.get("tags") -%}
        ALTER {{"view" if model.config.materialized == "view" else "table"}} {{ this }}
          SET TBLPROPERTIES (
            'dune.public'='true',
            'dune.data_explorer.blockchains'= '{{ blockchains }}',     -- e.g., ["ethereum","solana"]
            'dune.data_explorer.category'='abstraction',
            'dune.data_explorer.abstraction.type'= '{{ spell_type }}', -- 'project' or 'sector'
            'dune.data_explorer.abstraction.name'= '{{ spell_name }}', -- 'aave' or 'uniswap'
            'dune.data_explorer.contributors'= '{{ contributors }}',   -- e.g., ["soispoke","jeff_dude"]
            'dune.vacuum' = '{"enabled":true}'
          )
    {%- else -%}
      {%- set properties = {
              'dune.public': 'true',
              'dune.data_explorer.blockchains':  blockchains | as_text,
              'dune.data_explorer.category': 'abstraction',
              'dune.data_explorer.abstraction.type': spell_type,
              'dune.data_explorer.abstraction.name': spell_name,
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
  {%- endif -%}
{%- endmacro -%}

{%- macro trino_properties(properties) -%}
  map_from_entries(ARRAY[
  {%- for key, value in properties.items() %}
      ROW('{{ key }}', '{{ value }}')
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor %}
  ])
{%- endmacro -%}
