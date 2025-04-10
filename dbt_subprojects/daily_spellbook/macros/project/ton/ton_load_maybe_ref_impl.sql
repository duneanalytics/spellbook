{%- macro ton_load_maybe_ref_impl()
-%}

CAST(IF({{ ton_preload_uint('state', 1) }} = 0, ROW(
    state.has_idx, state.size, state.original_cell_data,
    null {# cell_pointer #},
    0 {# refs #}, 0 {# exotic #}, 0 {# level_ #}, null {# current_cell_data #}, null {# refs_indexes #}, 0 {# cursor_bit_offset #}, 0 {# cursor_ref_offset #},
            state.output {# output #}
), ROW(
    state.has_idx, state.size, state.original_cell_data,
    {{ ton_get_ref_cell() }} {# cell_pointer #},
    0 {# refs #}, 0 {# exotic #}, 0 {# level_ #}, null {# current_cell_data #}, null {# refs_indexes #}, 0 {# cursor_bit_offset #}, 0 {# cursor_ref_offset #},
            state.output {# output #}
)) AS {{ ton_boc_state_type() }})

{%- endmacro -%}