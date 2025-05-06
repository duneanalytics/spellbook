{%- macro ton_begin_parse_impl()
-%}

CAST(ROW(
    state.has_idx, state.size, state.original_cell_data, state.cell_pointer,
    {# refs #} bitwise_and(varbinary_to_integer (varbinary_substring (state.cell_pointer, 1, 1)), 7),
    {# excotic #} bitwise_right_shift(bitwise_and(varbinary_to_integer (varbinary_substring (state.cell_pointer, 1, 1)), 16), 4),
    {# level_ #} bitwise_right_shift(varbinary_to_integer (varbinary_substring (state.cell_pointer, 1, 1)), 5),
    {# current_cell_data #} 
    varbinary_substring (state.cell_pointer, 3, bitwise_right_shift(varbinary_to_integer (varbinary_substring (state.cell_pointer, 2, 1)), 1) + 
    bitwise_and(varbinary_to_integer (varbinary_substring (state.cell_pointer, 2, 1)), 1)),
    {# refs_indexes #} varbinary_substring (state.cell_pointer, 3 + bitwise_right_shift(varbinary_to_integer (varbinary_substring (state.cell_pointer, 2, 1)), 1) + 
    bitwise_and(varbinary_to_integer (varbinary_substring (state.cell_pointer, 2, 1)), 1), 
    bitwise_and(varbinary_to_integer (varbinary_substring (state.cell_pointer, 1, 1)), 7) * state.size),
    {# cursor_bit_offset #} 0, {# state.cursor_ref_offset #} 0, state.output
) AS {{ ton_boc_state_type() }})

{%- endmacro -%}