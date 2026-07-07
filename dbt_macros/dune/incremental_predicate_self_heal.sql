{#
  Self-healing incremental lower bound (CUR2-3130).

  The standard incremental_predicate() anchors the window to now() - N. If a
  scheduled run is missed, or a source row lands late (ingested after its
  N-unit window has passed), that row falls permanently outside every future
  window and the table freezes. Four thorchain gold tables froze this way for
  ~2.5 months (last write 2026-05-05) while the raw source stayed fresh.

  This anchors the lower bound to the table's own high-water mark instead:
    <source_time> >= coalesce(max(<table_time>) from this, epoch) - interval N unit

  When the table is behind, max(<table_time>) stays low, so the floor stays low
  and the next run pulls the entire backlog; when healthy, max ~= now, so it
  behaves like the normal N-unit window. The N-unit buffer re-scans the tip for
  late / slightly-out-of-order arrivals. N reuses DBT_ENV_INCREMENTAL_TIME, so a
  one-off `--vars '{DBT_ENV_INCREMENTAL_TIME: 120}'` still widens it.

  Apply on the SOURCE side only, and set NO incremental_predicates in config: a
  merge dest predicate cannot reference the merge target in a subquery, and a
  static now()-based dest predicate paired with a max-from-this source bound is
  exactly the asymmetry that caused the #9421 duplicate-row bug. With no dest
  predicate the merge matches on unique_key across the whole table, so it stays
  dup-safe by construction. Fine here because these are small, sparse event
  tables where a full-target merge scan is cheap.
#}
{% macro incremental_predicate_self_heal(source_time, table_time_column) -%}
{{ source_time }} >= coalesce((select max({{ table_time_column }}) from {{ this }}), timestamp '1970-01-01') - interval '{{ var('DBT_ENV_INCREMENTAL_TIME') }}' {{ var('DBT_ENV_INCREMENTAL_TIME_UNIT') }}
{%- endmacro -%}
