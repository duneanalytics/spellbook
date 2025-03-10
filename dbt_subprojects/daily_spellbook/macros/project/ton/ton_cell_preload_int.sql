{%- macro ton_cell_preload_int(
        cell, cell_cursor, size
    )
-%}
IF(
    {{ ton_cell_preload_uint(cell, cell_cursor, 1) }} = 0,
    CAST({{ ton_cell_preload_uint(cell, ton_cell_skip_bits(cell_cursor, 1), size - 1) }} AS INT256),
    CAST({{ ton_cell_preload_uint(cell, ton_cell_skip_bits(cell_cursor, 1), size - 1) }} AS INT256) - bitwise_left_shift(UINT256 '1', {{ size }} - 1)
)

{%- endmacro -%}