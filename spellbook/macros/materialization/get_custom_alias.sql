
{#
    Renders a alias name given a custom alias name. If the custom
    alias name is none, then the resulting alias is just the filename of the
    model. If an alias override is specified, then that is used.

    This macro can be overriden in projects to define different semantics
    for rendering a alias name.

    Arguments:
    custom_alias_name: The custom alias name specified for a model, or none
    node: The available node that an alias is being generated for, or none

#}

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