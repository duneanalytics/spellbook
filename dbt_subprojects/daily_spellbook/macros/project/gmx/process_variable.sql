{% macro process_variable(var1, var2, data_type, offset=64, length=32, exception=false) %}
    {%- if data_type in ['address', 'bytes32'] -%}
        varbinary_substring(data, varbinary_position(data, to_utf8('{{ var1 }}')) - {{ offset }}, {{ length }}) AS {{ var2 }}
    {%- elif data_type in ['unsigned_integer','boolean'] -%}
        {% if exception -%}
            varbinary_to_uint256(varbinary_substring(data, varbinary_position(data, from_hex(to_hex(to_utf8('{{ var1 }}')) || '00')) - {{ offset }}, {{ length }})) AS {{ var2 }}
        {%- else -%}
            varbinary_to_uint256(varbinary_substring(data, varbinary_position(data, to_utf8('{{ var1 }}')) - {{ offset }}, {{ length }})) AS {{ var2 }}
        {%- endif %}
    {%- elif data_type == 'integer' -%}
        varbinary_to_int256(varbinary_substring(data, varbinary_position(data, to_utf8('{{ var1 }}')) - {{ offset }}, {{ length }})) AS {{ var2 }}
    {%- elif data_type == 'bytes' -%}
        varbinary_substring(data, 
            varbinary_position(data, to_utf8('{{ var1 }}')) + {{ offset }}, -- value position of key-value pair
            TRY_CAST(varbinary_to_uint256(varbinary_substring(data, varbinary_position(data, to_utf8('{{ var1 }}')) + {{ length }}, {{ length }})) AS BIGINT) -- the length in bytes
            ) AS {{ var2 }}
    {%- elif data_type == 'address_array_items' -%}
        varbinary_to_uint256(varbinary_substring(data, varbinary_position(data, to_utf8('{{ var1 }}')) + {{ offset }}, {{ length }})) AS {{ var2 }}_n
    {%- elif data_type == 'string' -%}
        {% if exception -%}
            from_utf8(varbinary_substring(data, varbinary_position(data, from_hex(to_hex(to_utf8('{{ var1 }}')) || '00')) + {{ offset }}, {{ length }})) AS {{ var2 }}
        {%- else -%}
            from_utf8(varbinary_substring(data, varbinary_position(data, to_utf8('{{ var1 }}')) + {{ offset }}, {{ length }})) AS {{ var2 }}
        {%- endif %}
    {%- endif -%}
{% endmacro %}