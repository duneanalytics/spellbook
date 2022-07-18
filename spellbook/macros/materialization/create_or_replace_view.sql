/* {#
       Core materialization implementation. BigQuery and Snowflake are similar
       because both can use `create or replace view` where the resulting view schema
       is not necessarily the same as the existing view. On Redshift, this would
       result in: ERROR:  cannot change number of columns in view

       This implementation is superior to the create_temp, swap_with_existing, drop_old
       paradigm because transactions don't run DDL queries atomically on Snowflake. By using
       `create or replace view`, the materialization becomes atomic in nature.
#} */

{% macro create_or_replace_view() %}

    {%- set identifier = model['alias'] -%}


    {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
    {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

    {{ run_hooks(pre_hooks) }}

  -- If there's a table with the same name and we weren't told to full refresh,
  -- that's an error. If we were told to full refresh, drop it. This behavior differs
  -- for Snowflake and BigQuery, so multiple dispatch is used.
    {%- if old_relation is not none and old_relation.is_table -%}
        {{ handle_existing_table(should_full_refresh(), old_relation) }}
    {%- endif -%}

     -- if a PR test, use delta live table
    {%- if target.schema.startswith("sha_") or target.schema.startswith("dbt_") -%}

        {%- set identifier = model['alias'] -%}
        {%- set target_relation = api.Relation.create(
         identifier=identifier, schema=model['schema'], database=database,
         type='view') -%}

      -- build model
        {% call statement('main') -%}
            {{ get_create_global_temp_view_as_sql(identifier,  sql) }}
        {%- endcall %}

        {{ return({'relations': [target_relation ]}) }}

    -- if not a PR test, follow normal view materialization
    {%- else -%}

        {%- set target_relation = api.Relation.create(
         identifier=identifier, schema=schema, database=database,
         type='view') -%}

      -- build model
        {% call statement('main') -%}
            {{ get_create_view_as_sql(target_relation, sql) }}
        {%- endcall %}

       {{ run_hooks(post_hooks) }}

       {{ return({'relations': [target_relation]}) }}

    {%- endif -%}

{% endmacro %}