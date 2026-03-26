{#-
  Dune override for dbt-trino's trino__get_merge_sql.
  Upstream: https://github.com/starburstdata/dbt-trino/blob/master/dbt/include/trino/macros/materializations/incremental.sql

  Everything else (materialization, get_incremental_tmp_relation_type,
  trino__get_delete_insert_merge_sql, trino__get_incremental_microbatch_sql)
  resolves to upstream via dbt macro dispatch.

  Adds merge_skip_unchanged model config: when true, the MERGE only updates
  rows where at least one tracked column actually changed (via IS DISTINCT FROM).
  Tracked columns = all dest columns minus unique_key, merge_exclude_columns,
  and CHANGE_TRACKING_EXCLUDED_COLUMNS (hardcoded below).
-#}

{#-- Kept in sync with upstream; [dune] blocks are the only additions. --#}
{% macro trino__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) -%}
    {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set dest_cols_csv_source = dest_cols_csv.split(', ') -%}
    {%- set merge_update_columns = config.get('merge_update_columns') -%}
    {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
    {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {#-- [dune] merge_skip_unchanged: skip updating rows where no tracked column changed.
         Tracked columns = all dest columns minus unique_key, merge_exclude_columns, and CHANGE_TRACKING_EXCLUDED_COLUMNS. --#}
    {%- set merge_skip_unchanged = config.get('merge_skip_unchanged', false) -%}
    {%- if merge_skip_unchanged %}
        {%- set unique_key_list = [unique_key] if unique_key is string else (unique_key if unique_key is sequence and unique_key is not mapping else []) -%}
        {%- set excluded_cols = merge_exclude_columns if merge_exclude_columns is not none else [] -%}
        {%- set change_tracking_excluded_columns = (unique_key_list + excluded_cols + ['_updated_at']) | map('lower') | list -%}
        {%- set change_tracking_columns = [] -%}
        {%- for col in dest_columns -%}
            {%- if col.name | lower not in change_tracking_excluded_columns -%}
                {%- do change_tracking_columns.append(col) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
            {% for key in unique_key %}
                {% set this_key_match %}
                    DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
                {% endset %}
                {% do predicates.append(this_key_match) %}
            {% endfor %}
        {% else %}
            {% set unique_key_match %}
                DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
            {% endset %}
            {% do predicates.append(unique_key_match) %}
        {% endif %}

        {{ sql_header if sql_header is not none }}

        merge into {{ target }} as DBT_INTERNAL_DEST
            using {{ source }} as DBT_INTERNAL_SOURCE
            on {{"(" ~ predicates | join(") and (") ~ ")"}}

        {#-- [dune] When merge_skip_unchanged, only update rows where at least one tracked column changed --#}
        {% if merge_skip_unchanged and (change_tracking_columns | length > 0) %}
        when matched and (
            {% for col in change_tracking_columns -%}
                DBT_INTERNAL_SOURCE.{{ adapter.quote(col.name) }} IS DISTINCT FROM DBT_INTERNAL_DEST.{{ adapter.quote(col.name) }}
                {%- if not loop.last %} OR {% endif -%}
            {%- endfor %}
        ) then update set
        {% else %}
        when matched then update set
        {% endif %}
            {% for column_name in update_columns -%}
                {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
                {%- if not loop.last %}, {%- endif %}
            {%- endfor %}

        when not matched then insert
            ({{ dest_cols_csv }})
        values
            ({% for dest_cols in dest_cols_csv_source -%}
                DBT_INTERNAL_SOURCE.{{ dest_cols }}
                {%- if not loop.last %}, {% endif %}
            {%- endfor %})

    {% else %}
        insert into {{ target }} ({{ dest_cols_csv }})
        (
            select {{ dest_cols_csv }}
            from {{ source }}
        )
    {% endif %}
{% endmacro %}
