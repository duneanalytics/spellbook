{% macro drop_old_tables() %}
    {%- set default_schema = target.schema -%}
    
    {%- set get_pr_schemas_sql -%}
        SHOW SCHEMAS LIKE 'dbt_cloud_pr_*'
    {%- endset -%}

    {%- set results = run_query(get_pr_schemas_sql) -%}

    {%- if execute -%}
        {%- for schema in results -%}

            {%- set drop_schema_sql -%}
                DROP SCHEMA IF EXISTS {{ schema['databaseName'] }} CASCADE
                {{ PRINT(schema['databaseName']) }}
            {%- endset -%}

            
            {{ drop_schema_sql }};
            {do run_query(drop_schema_sql)}

        {%- endfor -%}
    {%- endif -%}
{%- endmacro -%}
