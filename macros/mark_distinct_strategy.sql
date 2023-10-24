{% macro mark_distinct_strategy(value) %}
  {{ set_trino_session_property(true, 'mark_distinct_strategy', value) }}
{% endmacro %}
