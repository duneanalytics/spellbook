{% macro backfill_column_comments(unique_ids, dry_run=true) %}
  {# Explicit manifest IDs only: never select the whole graph or rebuild data. #}
  {% if not execute %}{{ return([]) }}{% endif %}
  {% if unique_ids is string or not unique_ids or dry_run is not boolean %}
    {{ exceptions.raise_compiler_error('Pass a nonempty list of model/source unique_ids and a boolean dry_run.') }}
  {% endif %}
  {% set statements = [] %}
  {% for unique_id in unique_ids %}
    {% set node = graph.nodes.get(unique_id) or graph.sources.get(unique_id) %}
    {% if not node or node.resource_type not in ['model', 'source'] or not node.relation_name
          or (node.resource_type == 'model' and node.config.materialized == 'ephemeral') %}
      {{ exceptions.raise_compiler_error('Not a physical model/source: ' ~ unique_id) }}
    {% endif %}
    {% for name, column in node.columns.items() %}
      {% set identifier = adapter.quote(name) if column.get('quote') else name %}
      {% set description = column.get('description', '') %}
      {% set comment = "'" ~ description | replace("'", "''") ~ "'" if description else 'null' %}
      {% do statements.append('comment on column ' ~ node.relation_name ~ '.' ~ identifier ~ ' is ' ~ comment) %}
    {% endfor %}
  {% endfor %}
  {# Validate all IDs before any write. Dry runs need no warehouse connection. #}
  {% for sql in statements %}
    {% do log(sql ~ ';', info=true) %}
    {% if not dry_run %}{% do run_query(sql) %}{% endif %}
  {% endfor %}
  {{ return(statements) }}
{% endmacro %}
