{%- macro ton_restart_parse_impl()
-%}

CAST(ROW(
    state.has_idx, state.size, state.original_cell_data, state.original_cell_data,
    0 {# refs #}, 0 {# exotic #}, 0 {# level_ #}, null {# current_cell_data #},
    null {# refs_indexes #}, 0 {# cursor_bit_offset #}, 0 {# cursor_ref_offset #},
            state.output {# output #}
) AS {{ ton_boc_state_type() }})

{%- endmacro -%}