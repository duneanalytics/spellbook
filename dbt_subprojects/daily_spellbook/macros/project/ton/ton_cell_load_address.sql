{%- macro ton_cell_load_address(
        cell, cell_cursor, size
    )
-%}
CAST(CASE
    {# addr_std$10 anycast:(Maybe Anycast) workchain_id:int8 address:bits256  = MsgAddressInt; #}
    WHEN {{ ton_cell_preload_uint(cell, cell_cursor, 2) }} = 2
        THEN
        CASE WHEN bitwise_and({{ ton_cell_preload_uint(cell, cell_cursor, 3) }}, 1) > 0 
            THEN ROW({{ cell_cursor }}.bit_offset + 3, {{ cell_cursor }}.ref_offset, 'non-standard address (anycast)')
        ELSE 
        ROW({{ cell_cursor }}.bit_offset + 3 + 8 + 256, {{ cell_cursor }}.ref_offset,
        format('%d', CAST({{ ton_cell_preload_uint(cell, ton_cell_skip_bits(cell_cursor, 3), 8) }} AS bigint) ) || ':' 
         || to_hex( CAST({{ ton_cell_preload_uint(cell, ton_cell_skip_bits(cell_cursor, 11), 256) }} as varbinary) )
        )
        
        END
    {# addr_var$11 anycast:(Maybe Anycast) addr_len:(## 9) workchain_id:int32 address:(bits addr_len) = MsgAddressInt; #}
    ELSE 
        ROW({{ cell_cursor }}.bit_offset + 2, {{ cell_cursor }}.ref_offset, 'non-standard address') {# addr_none, addr_extern, addr_var #}
END AS ROW(bit_offset bigint, ref_offset bigint, address varchar))

{%- endmacro -%}