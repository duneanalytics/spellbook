{# Macros for disabling listing of relations from the warehouse.
 Enabled by default for the spellbook profile.  Run dbt compile with the flag --vars 'no-relation-listing: "true"'
 on the command line to explicitly enable or disable this.
 This speeds up DBT compile times. It must not be used in combination with dbt run.
 #}
{% macro get_catalog(information_schema, schemas) -%}
  {% do log("get_catalog") %}
  {% do log(target) %}
  {% do log("information_schema") %}
  {% do log(information_schema) %}
  {%- if (var('no-relation-listing', 'false').lower() == 'true') or (target.profile_name == 'spellbook-local') or (target.type == 'databricks') -%}
    {{ return([]) }}
  {%- else -%}
    {{ return(adapter.dispatch('get_catalog')(information_schema, schemas)) }}
  {%- endif -%}
{%- endmacro %}

{% macro list_schemas(database) -%}
  {%- if (var('no-relation-listing', 'false').lower() == 'true') or (target.profile_name == 'spellbook-local') or (target.type == 'databricks') -%}
    {{ return([]) }}
  {%- else -%}
    {{ return(adapter.dispatch('list_schemas')(database)) }}
  {%- endif -%}
{%- endmacro %}

{% macro list_relations_without_caching(schema_relation) %}
  {% do log("schema_relation") %}
  {% do log(schema_relation) %}
  {%- if (var('no-relation-listing', 'false').lower() == 'true') or (target.profile_name == 'spellbook-local') or (target.type == 'databricks') -%}
    {{ return([]) }}
  {%- else -%}
    {{ return(adapter.dispatch('list_relations_without_caching')(schema_relation)) }}
  {%- endif -%}
{%- endmacro %}

{# {% macro databricks__set_relations_cache(schema_relation) %}
  {% do log("schema_relation") %}
  {% do log(schema_relation) %}
{%- endmacro %} #}