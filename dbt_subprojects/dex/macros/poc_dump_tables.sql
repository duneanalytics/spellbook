{% macro poc_dump_tables() %}
  {% set query %}
    select table_catalog, table_schema, table_name
    from information_schema.tables
    where table_schema not in ('information_schema')
    limit 50
  {% endset %}
  {% set results = run_query(query) %}
  {% if execute %}
    {{ log("POC TABLE LIST: " ~ results.rows, info=True) }}
  {% endif %}
{% endmacro %}
