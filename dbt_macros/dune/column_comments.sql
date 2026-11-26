{# Share the native persistence and manual operation's quoting, diff and literal rules. #}
{% macro column_comment_literal(value) %}
  {% if not value %}{{ return('null') }}{% endif %}
  {# dbt-trino treats any line containing two hyphens as a SQL comment, even in a literal. #}
  {% set escaped = value | replace('\\', '\\005C') | replace('-', '\\002D')
      | replace("'", "''") | replace('\n', '\\000A') | replace('\r', '\\000D') %}
  {{ return("U&'" ~ escaped ~ "'") }}
{% endmacro %}

{% macro column_comment_statements(relation, columns, strict=false) %}
  {% if not execute or not columns %}{{ return([]) }}{% endif %}
  {% set sql %}
    select column_name, comment
    from {{ adapter.quote(relation.database) }}.information_schema.columns
    where table_schema = {{ column_comment_literal(relation.schema) }}
      and table_name = {{ column_comment_literal(relation.identifier) }}
  {% endset %}
  {% set current = {} %}
  {% set names = {} %}
  {% for row in run_query(sql).rows %}
    {% do current.update({row[0]: row[1]}) %}
    {% do names.update({row[0] | lower: row[0]}) %}
  {% endfor %}
  {% set statements = [] %}
  {% for name, column in columns.items() %}
    {% set physical = names.get(name | lower) %}
    {% if physical not in current %}
      {% if strict %}
        {{ exceptions.raise_compiler_error('Documented column not found: ' ~ relation ~ '.' ~ name) }}
      {% endif %}
    {% else %}
      {% set description = column.get('description') or none %}
      {% if current[physical] != description %}
        {% do statements.append('comment on column ' ~ relation ~ '.' ~ adapter.quote(physical)
            ~ ' is ' ~ column_comment_literal(description)) %}
      {% endif %}
    {% endif %}
  {% endfor %}
  {{ return(statements) }}
{% endmacro %}

{# Match physical Trino names before dbt's case-sensitive validation of quoted columns. #}
{% macro trino__persist_docs(relation, model, for_relation=true, for_columns=true) %}
  {% do dbt.default__persist_docs(relation, model, for_relation, false) %}
  {% if for_columns and config.persist_column_docs() and model.columns %}
    {% for sql in column_comment_statements(relation, model.columns) %}
      {% do run_query(sql) %}
    {% endfor %}
  {% endif %}
{% endmacro %}
