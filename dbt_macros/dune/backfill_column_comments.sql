{% macro backfill_column_comments(unique_ids, dry_run=true) %}
  {# Explicit manifest IDs only: never select the whole graph or rebuild data. #}
  {% if not execute %}{{ return([]) }}{% endif %}
  {% if unique_ids is string or not unique_ids or dry_run is not boolean %}
    {{ exceptions.raise_compiler_error('Pass a nonempty list of model unique_ids and a boolean dry_run.') }}
  {% endif %}
  {% set nodes = [] %}
  {% set statements = [] %}
  {% for unique_id in unique_ids %}
    {% set node = graph.nodes.get(unique_id) %}
    {% if not node or node.resource_type != 'model' or not node.relation_name
          or node.config.materialized not in ['table', 'incremental', 'view'] %}
      {{ exceptions.raise_compiler_error('Not a physical model: ' ~ unique_id) }}
    {% endif %}
    {% if node not in nodes %}{% do nodes.append(node) %}{% endif %}
  {% endfor %}
  {% for node in nodes %}
    {% if dry_run %}
      {# Offline preview is a candidate list, not a physical-column or change check. #}
      {% for name, column in node.columns.items() %}
        {% set physical = name if column.get('quote') else name | lower %}
        {% do statements.append(column_comment_statement(node.relation_name, physical, column.get('description'))) %}
      {% endfor %}
    {% else %}
      {% set relation = adapter.get_relation(database=node.database, schema=node.schema, identifier=node.alias) %}
      {% if relation is none %}
        {% do log('backfill_column_comments: skipping missing relation ' ~ node.relation_name, info=true) %}
      {% else %}
        {% do statements.extend(column_comment_statements(relation, node.columns, strict=true)) %}
      {% endif %}
    {% endif %}
  {% endfor %}
  {# All selected models' physical columns are checked before the first write. #}
  {% for sql in statements %}
    {% do log(sql ~ ';', info=true) %}
    {% if not dry_run %}{% do run_query(sql) %}{% endif %}
  {% endfor %}
  {{ return(statements) }}
{% endmacro %}
