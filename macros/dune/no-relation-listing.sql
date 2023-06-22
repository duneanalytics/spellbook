{# Macros for disabling listing of relations from the warehouse.
 Run dbt compile with the flag --vars 'no-relation-listing: "true"' on the command line to enable this.
 This speeds up DBT compile times. It must not be used in combination with dbt run.
 #}
{% macro get_catalog(information_schema, schemas) -%}
  {%- if var('no-relation-listing', 'false').lower() != 'true' -%}
    {{ return(adapter.dispatch('get_catalog')(information_schema, schemas)) }}
  {%- else -%}
    {{ return([]) }}
  {%- endif -%}
{%- endmacro %}

{% macro list_schemas(database) -%}
  {%- if var('no-relation-listing', 'false').lower() != 'true' -%}
    {{ return(adapter.dispatch('list_schemas')(database)) }}
  {%- else -%}
    {{ return([]) }}
  {%- endif -%}
{%- endmacro %}

{% macro list_relations_without_caching(schema_relation) %}
  {%- if var('no-relation-listing', 'false').lower() != 'true' -%}
    {{ return(adapter.dispatch('list_relations_without_caching')(schema_relation)) }}
  {%- else -%}
    {{ return([]) }}
  {%- endif -%}
{%- endmacro %}
