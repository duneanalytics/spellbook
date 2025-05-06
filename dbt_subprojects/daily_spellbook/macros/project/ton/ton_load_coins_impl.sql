{%- macro ton_load_coins_impl(field_name)
-%}
CAST(
    ROW(
      state.has_idx, state.size, state.original_cell_data, state.cell_pointer,
      state.refs, state.exotic, state.level_,
      state.current_cell_data, 
      state.refs_indexes,
      state.cursor_bit_offset + 4 + CAST(8 * {{ ton_preload_uint('state', 4) }} AS bigint), state.cursor_ref_offset,
      map_concat(state.output,
      map_from_entries(ARRAY[({{ field_name }}, CAST(CAST(
        bitwise_and(
          bitwise_right_shift(
              varbinary_to_uint256 (
                  varbinary_substring (
                      state.current_cell_data,
                      1 + cast(floor((state.cursor_bit_offset + 4) / 8e0) as bigint),
                      CAST({{ ton_preload_uint('state', 4) }} AS bigint) +
                      IF((state.cursor_bit_offset + 4) % 8 = 0 OR cast(floor((state.cursor_bit_offset + 4) / 8e0) as bigint) = cast(floor((state.cursor_bit_offset + 3 + 8 * {{ ton_preload_uint('state', 4) }}) / 8e0) as bigint), 0, 1) {# check that end of the slice in the next octet#}
                  )
              ),
              IF((4 + state.cursor_bit_offset) % 8 = 0, 0, 8 - (4 + state.cursor_bit_offset) % 8)),
          bitwise_left_shift(UINT256 '1', CAST(8 * {{ ton_preload_uint('state', 4) }} AS bigint)) - 1
        ) AS varchar)
         AS JSON))]))
    )
AS {{ ton_boc_state_type() }})

{%- endmacro -%}