{%- macro ton_cell_skip_bits(
        cell_cursor, offset
    )
-%}
CAST(ROW({{ cell_cursor }}.bit_offset + {{ offset }}, {{ cell_cursor }}.ref_offset) AS ROW(bit_offset bigint, ref_offset bigint))

{%- endmacro -%}