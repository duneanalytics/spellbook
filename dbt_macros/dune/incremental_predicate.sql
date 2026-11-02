{# DEPRECATED wrapper. The window contract lives in window.sql: incremental_filter(column),
   window_start(), window_end(). This name stays so existing call sites keep working; new
   models use the three macros directly.

   Explicit window input is DBT_EVENT_TIME_START / DBT_EVENT_TIME_END (half open), which the
   dbtco deploy flow exports for a windowed run. Rolling runs render the same
   `date_trunc(unit, now() - interval N unit)` lower bound as before. #}

{# deprecated: use incremental_filter('DBT_INTERNAL_DEST.<col>') in incremental_predicates #}
{% macro incremental_predicate(column) -%}
{{ return(incremental_filter(column)) }}
{%- endmacro %}
