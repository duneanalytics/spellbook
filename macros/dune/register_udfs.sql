{% macro register_udfs() %}


{% do run_query(bytea2numeric_v3()) %};

{% endmacro %}
