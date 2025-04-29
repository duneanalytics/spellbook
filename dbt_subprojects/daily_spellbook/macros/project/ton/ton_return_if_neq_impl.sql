{%- macro ton_return_if_neq_impl(value, expected)
-%}
CAST(
CASE
    WHEN CAST(state.output[{{ value }}]  AS VARCHAR) != {{ expected }} THEN
    ROW(
        state.has_idx, state.size, state.original_cell_data, state.cell_pointer,
        state.refs, state.exotic, state.level_,
        state.current_cell_data, 
        state.refs_indexes,
        state.cursor_bit_offset, state.cursor_ref_offset, map_concat(state.output,map(array[{{ton_action_return_flag()}}], array[CAST(true AS JSON)]))
        {# push return flag to the output array if condition is met #}
    )
    ELSE state
END
 AS {{ ton_boc_state_type() }})

{%- endmacro -%}