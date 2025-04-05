{% macro expose_spells(blockchains, spell_type, spell_name, contributors) %}
  {%- set validated_contributors = tojson(fromjson(contributors | as_text)) -%}
  {%- if ("%s" % validated_contributors) == "null" -%}
    {%- do exceptions.raise_compiler_error("Invalid contributors '%s'. The list of contributors must be valid JSON." % contributors) -%}
  {%- endif -%}
  {%- if target.name == 'prod' -%}
    {%- set properties = {
            'dune.public': 'true',
            'dune.data_explorer.blockchains':  blockchains | as_text,
            'dune.data_explorer.category': 'abstraction',
            'dune.data_explorer.abstraction.type': spell_type,
            'dune.data_explorer.abstraction.name': spell_name,
            'dune.data_explorer.contributors': validated_contributors,
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

{%- macro trino_properties(properties) -%}
  map_from_entries(ARRAY[
  {%- for key, value in properties.items() %}
      ROW('{{ key }}', '{{ value }}')
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor %}
  ])
{%- endmacro -%}
