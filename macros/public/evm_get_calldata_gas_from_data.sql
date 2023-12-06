{% macro evm_get_calldata_gas_from_data(data_field) %}
      16 * ( bytearray_length({{data_field}}) - (length(from_utf8({{data_field}})) - length(replace(from_utf8({{data_field}}), chr(0), ''))) ) --nonzero bytes
                + 4 * ( (length(from_utf8({{data_field}})) - length(replace(from_utf8({{data_field}}), chr(0), ''))) )
{% endmacro %}

--run macro
   -- create or replace function evm_get_calldata_gas_from_data(data_field VARBINARY)
   -- returns INTEGER
   -- return
   -- SELECT