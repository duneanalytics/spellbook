{% macro enforce_join_distribution(value) %}
  {{ set_trino_session_property(true, 'join_distribution_type', value) }}
{% endmacro %}
