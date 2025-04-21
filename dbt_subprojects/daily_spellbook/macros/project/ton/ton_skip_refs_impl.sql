{%- macro ton_skip_refs_impl(offset)
-%}
CAST(ROW(
    state.has_idx, state.size, state.original_cell_data, state.cell_pointer,
    state.refs, state.exotic, state.level_,
    state.current_cell_data, 
    state.refs_indexes,
    state.cursor_bit_offset, state.cursor_ref_offset + {{ offset }}, state.output
) AS {{ ton_boc_state_type() }})

{%- endmacro -%}