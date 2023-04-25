{% macro trino_comment(schema, table, columns) -%}
   {%- set view_select_column_names = columns|map(attribute='name')|join('", "') -%}
   {%- set view_select_statement = 'SELECT "%s" FROM "%s"."%s"' % (view_select_column_names, schema, table) -%}
   {%- set view_struct = {
     "originalSql": view_select_statement,
     "catalog": "delta_prod",
     "schema": schema,
     "columns": columns
   } -%}
   {%- set json_data = tojson(view_struct) -%}

    /* Presto View: {{base64(json_data)}} */
{% endmacro %}
