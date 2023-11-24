{% macro get_incremental_tmp_relation_type(strategy, unique_key, language) %}

    /* {#
       If we are running multiple statements (DELETE + INSERT),
       we must first save the model query results as a temporary table
       in order to guarantee consistent inputs to both statements.

       If we are running a single statement (MERGE or INSERT alone),
       we can save the model query definition as a view instead,
       for faster overall incremental processing.
  #} */
    {%- set views_enabled = config.get("views_enabled", true) -%}

    {% if language == "sql" and (
        views_enabled
        and (
            strategy in ("default", "append", "merge") or (unique_key is none)
        )
    ) %}
        {{ return("view") }}
    {% else %}  {#- -  play it safe -- #}
        {{ return("table") }}
    {% endif %}
{% endmacro %}

{% materialization incremental, adapter = "trino", supported_languages = ["sql"] -%}

    {#- - Set vars --#}
    {%- set full_refresh_mode = should_full_refresh() -%}
    {%- set language = model["language"] -%}
    {% set target_relation = this.incorporate(type="table") %}
    {% set existing_relation = load_relation(this) %}

    {#- - The temp relation will be a view (faster) or temp table, depending on upsert/merge strategy --#}
    {%- set unique_key = config.get("unique_key") -%}
    {% set incremental_strategy = config.get("incremental_strategy") or "default" %}
    {% set tmp_relation_type = get_incremental_tmp_relation_type(
        incremental_strategy, unique_key, language
    ) %}
    {% set tmp_relation = make_temp_relation(this).incorporate(
        type=tmp_relation_type
    ) %}
    -- the temp_ relation should not already exist in the database; get_relation
    -- will return None in that case. Otherwise, we get a relation that we can drop
    -- later, before we try to use this name for the current operation.
    {%- set preexisting_tmp_relation = load_cached_relation(tmp_relation) -%}

    {% set grant_config = config.get("grants") %}

    {% set on_schema_change = incremental_validate_on_schema_change(
        config.get("on_schema_change"), default="ignore"
    ) %}

    -- drop the temp relation if it exists already in the database
    {{ drop_relation_if_exists(preexisting_tmp_relation) }}

    {{ run_hooks(pre_hooks) }}

    {% if existing_relation is none %}
        {%- call statement("main", language=language) -%}
            {{ create_table_as(False, target_relation, compiled_code, language) }}
        {%- endcall -%}

    {% elif existing_relation.is_view %}
        {#- - Can't overwrite a view with a table - we must drop --#}
        {{
            log(
                "Dropping relation "
                ~ target_relation
                ~ " because it is a view and this model is a table."
            )
        }}
        {% do adapter.drop_relation(existing_relation) %}
        {%- call statement("main", language=language) -%}
            {{ create_table_as(False, target_relation, compiled_code, language) }}
        {%- endcall -%}
    {% elif full_refresh_mode %}
        -- Full Refresh happens here. The original dbt-trino implementation drops the table before recreating it, 
        -- which causes downtime + data loss if the job crashes while recreating.
        -- Instead, we use a temporary table to store the new data, then rename it to the target table.
        -- Which is the logic used for materialized table recreation.
        {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
        -- the intermediate_relation should not already exist in the database;
        -- get_relation
        -- will return None in that case. Otherwise, we get a relation that we can drop
        -- later, before we try to use this name for the current operation
        {%- set preexisting_intermediate_relation = load_cached_relation(
            intermediate_relation
        ) -%}

        {%- set backup_relation_type = (
            "table" if existing_relation is none else existing_relation.type
        ) -%}
        {%- set backup_relation = make_backup_relation(
            target_relation, backup_relation_type
        ) -%}
        -- as above, the backup_relation should not already exist
        {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}

        -- drop the temp relations if they exist already in the database
        {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
        {{ drop_relation_if_exists(preexisting_backup_relation) }}

        -- Execute full refresh in intermediate table
        {% call statement("main") -%}
            {{ create_table_as(False, intermediate_relation, sql) }}
        {%- endcall %}

        {#- - cleanup #}
        -- renaming the table to the backup name
        {% if existing_relation is not none %}
            {{ adapter.rename_relation(existing_relation, backup_relation) }}
        {% endif %}

        {{ adapter.rename_relation(intermediate_relation, target_relation) }}

        {#- - finally, drop the existing/backup relation after the commit #}
        {{ drop_relation_if_exists(backup_relation) }}

    {% else %}
        {#- - Create the temp relation, either as a view or as a temp table --#}
        {% if tmp_relation_type == "view" %}
            {%- call statement("create_tmp_relation") -%}
                {{ create_view_as(tmp_relation, compiled_code) }}
            {%- endcall -%}
        {% else %}
            {%- call statement("create_tmp_relation", language=language) -%}
                {{ create_table_as(True, tmp_relation, compiled_code, language) }}
            {%- endcall -%}
        {% endif %}

        {% do adapter.expand_target_column_types(
            from_relation=tmp_relation, to_relation=target_relation
        ) %}
        {#- - Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
        {% set dest_columns = process_schema_changes(
            on_schema_change, tmp_relation, existing_relation
        ) %}
        {% if not dest_columns %}
            {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
        {% endif %}

        {#- - Get the incremental_strategy, the macro to use for the strategy, and build the sql --#}
        {% set incremental_predicates = config.get("predicates", none) or config.get(
            "incremental_predicates", none
        ) %}
        {% set strategy_sql_macro_func = adapter.get_incremental_strategy_macro(
            context, incremental_strategy
        ) %}
        {% set strategy_arg_dict = {
            "target_relation": target_relation,
            "temp_relation": tmp_relation,
            "unique_key": unique_key,
            "dest_columns": dest_columns,
            "incremental_predicates": incremental_predicates,
        } %}

        {%- call statement("main") -%} {{ strategy_sql_macro_func(strategy_arg_dict) }}
        {%- endcall -%}
    {% endif %}

    {% do drop_relation_if_exists(tmp_relation) %}

    {{ run_hooks(post_hooks) }}

    {% set should_revoke = should_revoke(
        existing_relation.is_table, full_refresh_mode
    ) %}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

    {% do persist_docs(target_relation, model) %}

    {{ return({"relations": [target_relation]}) }}

{%- endmaterialization %}

{% macro trino__get_delete_insert_merge_sql(
    target, source, unique_key, dest_columns, incremental_predicates
) -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not string %}
            delete from {{ target }}
            where
                {% for key in unique_key %}
                    {{ target }}.{{ key }} in (select {{ key }} from {{ source }})
                    {{ "and " if not loop.last }}
                {% endfor %}
                {% if incremental_predicates %}
                    {% for predicate in incremental_predicates %}
                        and {{ predicate }}
                    {% endfor %}
                {% endif %}
            ;
        {% else %}
            delete from {{ target }}
            where
                ({{ unique_key }}) in (select {{ unique_key }} from {{ source }})
                {%- if incremental_predicates %}
                    {% for predicate in incremental_predicates %}
                        and {{ predicate }}
                    {% endfor %}
                {%- endif -%}
            ;

        {% endif %}
    {% endif %}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    )
{%- endmacro %}

{% macro trino__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) -%}
    {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set dest_cols_csv_source = dest_cols_csv.split(', ') -%}
    {%- set merge_update_columns = config.get('merge_update_columns') -%}
    {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
    {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

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

        {% if unique_key %}
        when matched then update set
            {% for column_name in update_columns -%}
                {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
                {%- if not loop.last %}, {%- endif %}
            {%- endfor %}
        {% endif %}

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
