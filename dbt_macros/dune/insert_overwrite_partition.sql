{% macro insert_overwrite_partition(value) %}
  {{ set_trino_session_property(true, 'insert-existing-partitions-behavior', value) }}
{% endmacro %}