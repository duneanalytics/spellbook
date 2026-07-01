{#-
  delta_cdf incremental strategy. Dispatched by the trino incremental materialization
  because a model sets incremental_strategy='delta_cdf' and dbt finds this macro by name
  (impl.py:1782 -> get_incremental_<strategy>_sql). 'delta_cdf' is not a builtin, so the
  adapter allowlist check is skipped (impl.py:1776). No adapter fork.

  The temp_relation is a MATERIALIZED TABLE for this strategy (incremental.sql:14/17:
  delta_cdf is not in default/append/merge, and unique_key is set), so probing it is a
  cheap scan, not a feed re-execution. The model body (via transfers_enrich_cdf ->
  source_changes) carries _change_type and _commit_version into temp on the incremental
  path.

  Steps:
    1. run_query max(_commit_version) of the applied change set (and assert no unexpected
       deletes: spellbook merge bases never emit delete change rows).
    2. dedup temp to the latest change per unique_key (MERGE requires <=1 source row per
       target row, and a key can change across multiple commits in one window).
    3. MERGE upsert against dest_columns; the extra _change_type/_commit_version temp
       columns are inert (on_schema_change='ignore' keeps dest_columns = target columns).
       No time-window incremental_predicates on the ON clause (CDF can legitimately update
       arbitrarily old rows). Instead the ON clause is augmented with a partition-range
       bound DBT_INTERNAL_DEST.<partcol> BETWEEN min..max read from the change set: a
       changed row's partition value is immutable, so this prunes only target partitions
       that cannot contain a match (never dropping a real match) and bounds the target
       MERGE scan to the touched partitions.
    4. emit "MERGE ... ; ALTER TABLE ... SET PROPERTIES dune.cdf.source_version=<max_v>"
       as two statements (dbt-trino splits on ';' via sqlparse, connections.py:547).
       Skip the ALTER on an empty feed (max_v IS NULL) so the watermark holds.
-#}
{% macro get_incremental_delta_cdf_sql(arg_dict) %}
  {{ return(trino__get_incremental_delta_cdf_sql(arg_dict)) }}
{% endmacro %}

{% macro trino__get_incremental_delta_cdf_sql(arg_dict) -%}
  {%- set target = arg_dict["target_relation"] -%}
  {%- set temp = arg_dict["temp_relation"] -%}
  {%- set unique_key = arg_dict["unique_key"] -%}
  {%- set dest_columns = arg_dict["dest_columns"] -%}
  {%- set cdf_apply_deletes = config.get('cdf_apply_deletes', false) -%}

  {%- if unique_key is string -%}
    {%- set unique_key_cols = [unique_key] -%}
  {%- else -%}
    {%- set unique_key_cols = unique_key -%}
  {%- endif -%}

  {#-- partition columns are immutable per row -> their change-set range prunes the target scan --#}
  {%- set partition_by = config.get('partition_by') -%}
  {%- if partition_by is string -%}
    {%- set partition_cols = [partition_by] -%}
  {%- elif partition_by -%}
    {%- set partition_cols = partition_by -%}
  {%- else -%}
    {%- set partition_cols = [] -%}
  {%- endif -%}

  {#-- 1. capture applied max(_commit_version) + partition-range bounds; assert no unexpected deletes --#}
  {%- set max_v = none -%}
  {%- set prune_preds = [] -%}
  {%- if execute -%}
    {%- set probe -%}
      select max(_commit_version) as max_v, count_if(_change_type = 'delete') as n_del
      {%- for pc in partition_cols %}, min({{ pc }}) as min_{{ loop.index0 }}, max({{ pc }}) as max_{{ loop.index0 }}{% endfor %}
      from {{ temp }}
    {%- endset -%}
    {%- set res = run_query(probe) -%}
    {%- if res is not none and res.rows | length > 0 -%}
      {%- set max_v = res.rows[0][0] -%}
      {%- set n_del = res.rows[0][1] -%}
      {%- if n_del is not none and n_del | int > 0 and not cdf_apply_deletes -%}
        {{ exceptions.raise_compiler_error("delta_cdf: " ~ n_del ~ " delete change rows reached " ~ target ~ " but cdf_apply_deletes is false. spellbook merge bases never delete; investigate the source feed.") }}
      {%- endif -%}
      {%- for pc in partition_cols -%}
        {%- set lo = res.rows[0][2 + 2 * loop.index0] -%}
        {%- set hi = res.rows[0][3 + 2 * loop.index0] -%}
        {%- if lo is not none and hi is not none -%}
          {%- do prune_preds.append("DBT_INTERNAL_DEST." ~ adapter.quote(pc) ~ " between " ~ cdf_partition_literal(lo) ~ " and " ~ cdf_partition_literal(hi)) -%}
        {%- endif -%}
      {%- endfor -%}
    {%- endif -%}
  {%- endif -%}

  {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
  {%- set dest_cols_list = dest_cols_csv.split(', ') -%}
  {%- set update_columns = get_merge_update_columns(config.get('merge_update_columns'), config.get('merge_exclude_columns'), dest_columns) -%}

  {#-- 2. dedup temp to latest change per unique_key --#}
  {%- set deduped_source -%}
    select {{ dest_cols_csv }}
    from (
      select {{ dest_cols_csv }}, _change_type, _commit_version,
             row_number() over (
               partition by {{ unique_key_cols | join(', ') }}
               order by _commit_version desc, _change_type desc
             ) as _cdf_rn
      from {{ temp }}
      {%- if not cdf_apply_deletes %}
      where _change_type <> 'delete'
      {%- endif %}
    )
    where _cdf_rn = 1
  {%- endset -%}

  {#-- 3. MERGE upsert --#}
  merge into {{ target }} as DBT_INTERNAL_DEST
    using ( {{ deduped_source }} ) as DBT_INTERNAL_SOURCE
    on {% for k in unique_key_cols %}(DBT_INTERNAL_SOURCE.{{ k }} = DBT_INTERNAL_DEST.{{ k }}){% if not loop.last %} and {% endif %}{% endfor %}{% for p in prune_preds %} and {{ p }}{% endfor %}
  when matched then update set
    {% for column_name in update_columns -%}
      {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}{% if not loop.last %}, {% endif %}
    {%- endfor %}
  when not matched then insert
    ({{ dest_cols_csv }})
    values
    ({% for col in dest_cols_list -%}
      DBT_INTERNAL_SOURCE.{{ col }}{% if not loop.last %}, {% endif %}
    {%- endfor %})
  {%- if execute and max_v is not none %}
  ;
  {#-- 4. advance the watermark to the exact max applied version --#}
  {{ cdf_advance_watermark(target, max_v | int) }}
  {%- endif %}
{%- endmacro %}

{#-- render a partition-column value as a Trino literal for the prune predicate. Avoids
    dunder access (dbt's Jinja sandbox blocks __class__): a date/timestamp falls through to
    the string-repr branch and is wrapped by date '' / timestamp '' based on a time part. --#}
{% macro cdf_partition_literal(v) -%}
  {%- if v is none -%}null
  {%- elif v is string -%}'{{ v }}'
  {%- elif v is number -%}{{ v }}
  {%- else -%}
    {%- set s = v | string -%}
    {%- if ':' in s -%}timestamp '{{ s }}'{%- else -%}date '{{ s }}'{%- endif -%}
  {%- endif -%}
{%- endmacro %}
