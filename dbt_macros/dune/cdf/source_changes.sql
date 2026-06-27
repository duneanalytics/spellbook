{#-
  Expand a ref-based Delta spell source into a CDF change-set relation for a
  delta_cdf incremental model. Carries _change_type / _commit_version /
  _commit_timestamp through so the strategy macro (Task B) can capture the applied
  max(_commit_version). Three branches:

    - parse mode (not execute): a static, compilable stub. No introspection.
    - bootstrap (target absent OR --full-refresh => is_incremental() is false): full
      snapshot of the base pinned at its current version V (FOR VERSION AS OF V), every
      row tagged 'insert', _commit_version = V.
    - incremental: <catalog>.system.table_changes(since_version => W), W = the stored
      watermark. since_version is an EXCLUSIVE lower bound, so the next run reads strictly
      after the last applied version (no overlap, no skip).

  Column contract: both real branches append, in order, payload cols..., _change_type,
  _commit_version, _commit_timestamp. The CONSUMING model must keep _commit_version in
  its FINAL projection only on the incremental path (is_incremental()); on bootstrap it
  must drop the CDF metadata columns so the target table schema stays clean (the
  bootstrap CTAS output becomes the table verbatim).

  The catalog is taken from base_relation.database (NOT hardcoded delta_prod) so this
  works in dev/CI where ref() resolves to the dev catalog. table_changes takes
  schema_name + table_name only; the catalog lives in the function path.

  Trino-Delta specific; the worker is named trino__ for a future adapter.dispatch swap.
-#}
{% macro source_changes(base_relation, change_types=['insert', 'update_postimage']) -%}
{{ trino__source_changes(base_relation, change_types) }}
{%- endmacro %}

{% macro trino__source_changes(base_relation, change_types) -%}
{%- if not execute -%}
{#-- parse-mode stub: valid, compilable, no introspection --#}
select *
  , cast('insert' as varchar) as _change_type
  , cast(0 as bigint) as _commit_version
  , cast(null as timestamp(3) with time zone) as _commit_timestamp
from {{ base_relation }}
{%- elif not is_incremental() -%}
{#-- bootstrap: full snapshot pinned at the source's current version, tagged as inserts --#}
{%- set v = cdf_current_source_version(base_relation) -%}
{%- if v is none -%}
{{ exceptions.raise_compiler_error("source_changes: cannot resolve $history version to bootstrap " ~ base_relation ~ " (enable change_data_feed_enabled on the source and ensure it is built first)") }}
{%- endif -%}
select *
  , cast('insert' as varchar) as _change_type
  , cast({{ v }} as bigint) as _commit_version
  , cast(null as timestamp(3) with time zone) as _commit_timestamp
from {{ base_relation }} for version as of {{ v }}
{%- else -%}
{#-- incremental: change feed strictly after the stored watermark.
     table_changes() returns Dune uint256/int256 columns as their raw big-endian
     varbinary (the logical type annotation is lost through the table function), unlike a
     normal scan. Re-decode those columns via bytearray_to_uint256 / bytearray_to_int256
     so the feed schema is identical to the bootstrap / base output; every other column
     (incl. genuine varbinary like addresses/hashes) passes through untouched. Column
     types come from information_schema, where the logical uint256/int256 IS preserved. --#}
{%- set w = cdf_get_watermark(this) -%}
{%- if w is none -%}
{{ exceptions.raise_compiler_error("source_changes: target " ~ this ~ " is incremental but has no dune.cdf.source_version watermark; run with --full-refresh to bootstrap") }}
{%- endif -%}
{%- set change_types_csv = "'" ~ (change_types | join("', '")) ~ "'" -%}
{%- set col_rows = run_query(
      "select column_name, data_type from " ~ base_relation.database
      ~ ".information_schema.columns where table_schema = '" ~ base_relation.schema
      ~ "' and table_name = '" ~ base_relation.identifier ~ "' order by ordinal_position") -%}
{%- set decoded = [] -%}
{%- for r in col_rows.rows -%}
  {%- set cname = r[0] -%}
  {%- set ctype = (r[1] | lower) -%}
  {%- if 'uint256' in ctype -%}
    {%- do decoded.append("bytearray_to_uint256(" ~ adapter.quote(cname) ~ ") as " ~ adapter.quote(cname)) -%}
  {%- elif 'int256' in ctype -%}
    {%- do decoded.append("bytearray_to_int256(" ~ adapter.quote(cname) ~ ") as " ~ adapter.quote(cname)) -%}
  {%- else -%}
    {%- do decoded.append(adapter.quote(cname)) -%}
  {%- endif -%}
{%- endfor -%}
select {{ decoded | join(', ') }}
  , _change_type, _commit_version, _commit_timestamp
from table({{ base_relation.database }}.system.table_changes(
  schema_name => '{{ base_relation.schema }}',
  table_name => '{{ base_relation.identifier }}',
  since_version => {{ w | int }}
))
where _change_type in ({{ change_types_csv }})
{%- endif -%}
{%- endmacro %}
