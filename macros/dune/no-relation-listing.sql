{# Macros for disabling listing of relations from the warehouse.
 Enabled by default for the spellbook profile.  Run dbt compile with the flag --vars 'no-relation-listing: "true"'
 on the command line to explicitly enable or disable this.
 This speeds up DBT compile times. It must not be used in combination with dbt run.

 databricks:  or (target.type == 'databricks')
 #}
{% macro get_catalog(information_schema, schemas) -%}
  {%- if (var('no-relation-listing', 'false').lower() == 'true') or (target.profile_name == 'spellbook-local') -%}
    {{ return([]) }}
  {%- else -%}
    {{ return(adapter.dispatch('get_catalog')(information_schema, schemas)) }}
  {%- endif -%}
{%- endmacro %}

{% macro list_schemas(database) -%}
  {% do log('model', info=True) %}
  {% do log(model, info=True) %}
  {% do log(this, info=True) %}
  {%- if (var('no-relation-listing', 'false').lower() == 'true') or (target.profile_name == 'spellbook-local') -%}
    {{ return([]) }}
  {%- else -%}
    {{ return(adapter.dispatch('list_schemas')(database)) }}
  {%- endif -%}
{%- endmacro %}

{% macro list_relations_without_caching(schema_relation) %}
  {% do log('list_relations_without_caching START', info=True) %}
  {% do log(model, info=True) %}
  {% do log(this, info=True) %}
  {% do log('schema_relation') %}
  {% do log(schema_relation) %}
  {% do log('list_relations_without_caching END', info=True) %}
  {%- if (var('no-relation-listing', 'false').lower() == 'true') or (target.profile_name == 'spellbook-local') -%}
    {{ return([]) }}
  {%- else -%}
    {{ return(adapter.dispatch('list_relations_without_caching')(schema_relation)) }}
  {%- endif -%}
{%- endmacro %}


--{% macro databricks__list_relations_without_caching(schema_relation) %}
--  {% do log('databricks__list_relations_without_caching') %}
--  {% do log(target.tags) %}
--  {% do log(target.get("tags")) %}
--  {%- if (var('no-relation-listing', 'false').lower() == 'true') or (target.profile_name == 'spellbook-local') -%}
--    {{ return([]) }}
--  {%- else -%}
--    {{ return(adapter.get_relations_without_caching(schema_relation)) }}
--  {%- endif -%}
--{% endmacro %}
