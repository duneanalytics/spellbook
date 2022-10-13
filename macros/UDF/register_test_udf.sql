{% macro register_test_udf() %}
    CREATE FUNCTION hex2dec(hex STRING)
    RETURNS STRING
    LANGUAGE PYTHON
    AS $$
    return str(int(hex,16))
    $$;
{% endmacro %}
