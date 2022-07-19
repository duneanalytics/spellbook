{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if target.name == 'prod' or target.schema == 'wizard' and custom_schema_name is not none -%}

        {{ custom_schema_name | trim }}

    {%- elif (target.schema.startswith("sha_") or target.schema.startswith("dbt_"))  and node.config.materialized == 'view' -%}

        {{ 'global_temp' }}

    {%- elif target.schema.startswith("sha_") or target.schema.startswith("dbt_")  and node.config.materialized in ('table', 'incremental', 'seed') -%}

         {{ 'delta' }}

    {%- elif custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
