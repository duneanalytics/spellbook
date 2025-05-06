{%- macro ton_skip_maybe_ref_impl()
-%}
CAST(ROW(
    state.has_idx, state.size, state.original_cell_data, state.cell_pointer,
    state.refs, state.exotic, state.level_,
    state.current_cell_data, 
    state.refs_indexes,
    state.cursor_bit_offset + 1, state.cursor_ref_offset + {{ ton_preload_uint('state', 1) }}, state.output
) AS {{ ton_boc_state_type() }})

{%- endmacro -%}