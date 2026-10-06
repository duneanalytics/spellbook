{%- macro trino_properties(properties) -%}
  map_from_entries(ARRAY[
  {%- for key, value in properties.items() %}
      ROW('{{ key }}', '{{ value | replace("'", "''") }}')
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor %}
  ])
{%- endmacro -%}

{%- macro apply_unique_key_columns(properties) -%}
  {%- set unique_key = model.config.get('unique_key') -%}
  {%- set overrides = (model.config.get('meta', {}) or {}).get('dune', {}) -%}
  {%- set has_override = overrides is mapping and 'unique_key_columns' in overrides -%}
  {%- if unique_key or has_override -%}
    {%- set columns = overrides['unique_key_columns'] if has_override else ([unique_key] if unique_key is string else unique_key) -%}
    {%- if columns is not sequence or columns is string or columns is mapping or columns | length == 0 -%}
      {%- do exceptions.raise_compiler_error("unique_key or meta.dune.unique_key_columns must resolve to a non-empty list of column names.") -%}
    {%- endif -%}
    {%- do properties.update({'dune.unique_key_columns': tojson(unique_key_column_names(columns))}) -%}
  {%- endif -%}
{%- endmacro -%}

{#- unique_key entries are SQL, but dune.unique_key_columns stores column names. Each entry must be
    a bare identifier or a double-quoted one, such as '"from"', which is stored unquoted. -#}
{%- macro unique_key_column_names(columns) -%}
  {%- set names = [] -%}
  {%- for column in columns -%}
    {%- if column is string and modules.re.fullmatch('[A-Za-z_][A-Za-z0-9_]*', column) -%}
      {%- do names.append(column) -%}
    {%- elif column is string and modules.re.fullmatch('"(?:[^"]|"")+"', column) -%}
      {%- do names.append(column[1:-1] | replace('""', '"')) -%}
    {%- else -%}
      {%- do exceptions.raise_compiler_error("unique_key or meta.dune.unique_key_columns must list column names, each bare or double-quoted; got " ~ column) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- do return(names) -%}
{%- endmacro -%}

{#
  The catalog service only derives filtering columns from a table's partition columns, so views
  and unpartitioned tables get no Data Explorer filtering hint unless the spell sets one. Every
  macro that emits properties has to apply this: on the table path, ALTER TABLE SET PROPERTIES
  replaces the whole data explorer metadata struct, so the last statement of a run must carry it.

  An explicit empty list is emitted rather than skipped. View property updates only upsert the
  keys they send, so `[]` is the only way to withdraw a hint that is already published.
#}
{%- macro apply_filtering_columns(properties) -%}
  {%- set columns = model.config.get('filtering_columns', none) -%}
  {%- if columns is not none -%}
    {%- if columns is not sequence or columns is mapping or columns is string or columns | reject('string') | list | length > 0 -%}
      {%- do exceptions.raise_compiler_error("Invalid filtering_columns '" ~ columns ~ "'. Must be a list of column names.") -%}
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
    {%- do apply_unique_key_columns(properties) -%}
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
    {%- do apply_unique_key_columns(properties) -%}
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
