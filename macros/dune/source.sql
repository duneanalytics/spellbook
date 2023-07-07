{% macro source(source_name, table_name) %}
  {% set rel = trino_source(source_name, table_name) %}
  {%- set time_column, test_dates = get_source_time_column(rel) -%}
  {% if time_column != None %}
    {%- do return(time_filter(rel, time_column, test_dates)) -%}
  {% else %}
    {% do log("time_column is None", info=True) %}
    {%- do return(rel) -%}
  {% endif %}
{% endmacro %}

{# Update database of sources to delta_prod when using Trino #}
{% macro trino_source(source_name, table_name) %}
  {% set rel = builtins.source(source_name, table_name) %}
  {%- if target.type == 'trino' -%}
    {%- set newrel = rel.replace_path(database="delta_prod") -%}
    {%- do return(newrel) -%}
  {%- else -%}
    {% do return(rel) %}
  {%- endif -%}
{% endmacro %}

{% macro time_filter(rel, time_column, test_dates) %}
  {% set predicates = [] %}
  {% for date in test_dates %}
    {% do predicates.append("(timestamp '%s 00:00' < %s and %s < timestamp '%s 01:00')" % (date, time_column, time_column, date)) %}
  {% endfor %}
  {%- do return('( select * from ' + (rel | string) + ' where ' + (predicates|join(" or "))  + ")") -%}
{% endmacro %}

{# Get the loaded_at_field. The column must be defined in the contract. #}
{% macro get_source_time_column(rel) %}
  {% if (not target.schema.startswith('github_action')) and (not ((var('fast', 'false') | string).lower() == 'true')) %}
    {% do log("Not fast mode", info=True) %}
    {% do return((None, [])) %}
  {% endif %}
  {% set key = "source.spellbook." + rel.schema + "." + rel.identifier %}
  {# Depending on the parsing stage graph can be empty, but it's alway non-empty when this is exceuted from macros #}
  {% set source_definition = graph.get('sources', {}).get(key, {}) %}
  {% set time_column = source_definition.get('loaded_at_field', None) %}
  {% set global_test_dates = source_definition.get('config', {}).get('meta', {}).get('loaded_at_field_test_dates', []) %}
  {% set test_dates = source_definition.get('meta', {}).get('loaded_at_field_test_dates', global_test_dates) %}
  {% do return((time_column, test_dates)) %}
{% endmacro %}
