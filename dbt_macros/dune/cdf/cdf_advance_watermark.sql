{#-
  Emit an ALTER TABLE ... SET PROPERTIES that stamps dune.cdf.source_version = <version>
  while preserving any existing dune.* extra_properties keys. SET PROPERTIES
  extra_properties REPLACES the whole custom map, so every dune.* key must be re-listed.
  Filtering key LIKE 'dune.%' isolates Dune's custom metadata from native delta.* keys.

  Accepts either a captured literal (incremental: max(_commit_version) of the applied
  set) or render-time V (bootstrap). In dev the only dune.* key present is the watermark
  itself (mark_as_spell / expose_spells are prod-gated no-ops), so the preserve-loop is
  empty; the loop keeps the write forward-compatible for the Phase 2 prod path.

  NOTE: values are interpolated as single-quoted literals; Dune's dune.* values are
  numbers / booleans / double-quoted JSON, so they contain no single quotes. Revisit if
  a single-quote-bearing key is ever stored here.
-#}
{% macro cdf_advance_watermark(target_relation, version) -%}
  {%- set entries = [] -%}
  {%- if execute -%}
    {%- set probe -%}
      select key, value
      from {{ target_relation.database }}.{{ target_relation.schema }}."{{ target_relation.identifier }}$properties"
      where key like 'dune.%' and key <> 'dune.cdf.source_version'
    {%- endset -%}
    {%- set existing = run_query(probe) -%}
    {%- if existing is not none -%}
      {%- for row in existing.rows -%}
        {%- do entries.append("ROW('" ~ row[0] ~ "', '" ~ row[1] ~ "')") -%}
      {%- endfor -%}
    {%- endif -%}
  {%- endif -%}
  {%- do entries.append("ROW('dune.cdf.source_version', '" ~ version ~ "')") -%}
  alter table {{ target_relation }} set properties extra_properties = map_from_entries(ARRAY[{{ entries | join(', ') }}])
{%- endmacro %}
