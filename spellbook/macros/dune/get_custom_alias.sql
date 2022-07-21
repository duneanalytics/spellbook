{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {% do return(adapter.dispatch('generate_alias_name', 'dbt')(custom_alias_name, node)) %}
{%- endmacro %}

{% macro default__generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- if (target.schema.startswith("sha_") or target.schema.startswith("dbt_")) and node.config.materialized in ('table', 'incremental', 'seed') -%}

        `/tmp/delta/{{target.schema}}/{{node.name}}`

    {%- elif (target.schema.startswith("sha_") or target.schema.startswith("dbt_")) and node.config.materialized in ('view') -%}

            {{ node.name }}

    {%- else -%}

        {%- if custom_alias_name is none -%}

            {{ node.name }}

        {%- else -%}

            {{ custom_alias_name | trim }}

        {%- endif -%}

    {%- endif -%}

{%- endmacro %}