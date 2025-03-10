{%- macro ton_cell_load_uint(
        cell, cell_cursor, size, cast_row=true
    )
-%}
{% if cast_row %}CAST({% endif %}ROW({{ cell_cursor }}.bit_offset + {{ size }}, {{ cell_cursor }}.ref_offset, {{ ton_cell_preload_uint(cell, cell_cursor, size) }}){% if cast_row %} AS ROW(bit_offset bigint, ref_offset bigint, value UINT256)){% endif %}

{%- endmacro -%}