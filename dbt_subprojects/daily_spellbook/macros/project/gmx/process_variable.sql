{% macro process_variable(var, data_type, offset=64, length=32) %}
    {%- set underscored_var = var | replace('.', '_') -%}
    {%- if data_type == 'address' -%}
        varbinary_substring(data, varbinary_position(data, to_utf8('{{ var }}')) - {{ offset }}, {{ length }}) AS {{ underscored_var }}
    {%- elif data_type == 'unsigned_integer' -%}
        varbinary_to_uint256(varbinary_substring(data, varbinary_position(data, to_utf8('{{ var }}')) - {{ offset }}, {{ length }})) AS {{ underscored_var }}
    {%- elif data_type == 'integer' -%}
        varbinary_to_int256(varbinary_substring(data, varbinary_position(data, to_utf8('{{ var }}')) - {{ offset }}, {{ length }})) AS {{ underscored_var }}
    {%- elif data_type == 'boolean' -%}
        varbinary_to_uint256(varbinary_substring(data, varbinary_position(data, to_utf8('{{ var }}')) - {{ offset }}, {{ length }})) AS {{ underscored_var }}
    {%- elif data_type == 'bytes32' -%}
        varbinary_substring(data, varbinary_position(data, to_utf8('{{ var }}')) - {{ offset }}, {{ length }}) AS {{ underscored_var }}
    {%- endif -%}
{% endmacro %}
