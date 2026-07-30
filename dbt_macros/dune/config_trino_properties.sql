{%- macro trino_properties(properties) -%}
  map_from_entries(ARRAY[
  {%- for key, value in properties.items() %}
      ROW('{{ key }}', '{{ value }}')
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor %}
  ])
{%- endmacro -%}

{#
  The catalog service only derives filtering columns from a table's partition columns, so views
  and unpartitioned tables get no Data Explorer filtering hint unless the spell sets one. Every
  macro that emits properties has to apply this: on the table path, ALTER TABLE SET PROPERTIES
  replaces the whole data explorer metadata struct, so the last statement of a run must carry it.
#}
{%- macro apply_filtering_columns(properties) -%}
  {%- set columns = model.config.get('filtering_columns', none) -%}
  {%- if columns -%}
    {%- if columns is not sequence or columns is mapping or columns is string or columns | reject('string') | list | length > 0 -%}
      {%- do exceptions.raise_compiler_error("Invalid filtering_columns '%s'. Must be a list of column names." % columns) -%}
    {%- endif -%}
    {%- do properties.update({'dune.data_explorer.filtering_columns': tojson(columns)}) -%}
  {%- endif -%}
{%- endmacro -%}

{% macro expose_spells(blockchains, spell_type, spell_name, contributors) %}
  {%- set validated_contributors = tojson(fromjson(contributors | as_text)) -%}
  {%- if ("%s" % validated_contributors) == "null" -%}
    {%- do exceptions.raise_compiler_error("Invalid contributors '%s'. The list of contributors must be valid JSON." % contributors) -%}
  {%- endif -%}
  {%- if target.name == 'prod' -%}
    {%- set properties = {
            'dune.created_by': 'dbt_spellbook',
            'dune.public': 'true',
            'dune.visible': 'true',
            'dune.data_explorer.blockchains':  blockchains | as_text,
            'dune.data_explorer.category': 'abstraction',
            'dune.data_explorer.abstraction.type': spell_type,
            'dune.data_explorer.abstraction.name': spell_name,
            'dune.data_explorer.contributors': validated_contributors,
            'dune.data_explorer.freshness': var('freshness'),
            'dune.vacuum': '{"enabled":true}'
          } -%}
    {%- do apply_filtering_columns(properties) -%}
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

{% macro hide_spells() %}
  {%- if target.name == 'prod' -%}
    {%- set properties = {
            'dune.created_by': 'dbt_spellbook',
            'dune.public': 'true',
            'dune.visible': 'false',
            'dune.data_explorer.category': 'abstraction',
            'dune.vacuum': '{"enabled":true}'
          } -%}
    {%- do apply_filtering_columns(properties) -%}
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