{%- macro ton_load_address_impl(field_name)
-%}
CAST(
CASE
    {# addr_none$00 = MsgAddressExt; #}
    WHEN {{ ton_preload_uint('state', 2) }} = 0 THEN
    ROW(
      state.has_idx, state.size, state.original_cell_data, state.cell_pointer,
      state.refs, state.exotic, state.level_,
      state.current_cell_data, 
      state.refs_indexes,
      state.cursor_bit_offset + 2, state.cursor_ref_offset,
      map_concat(state.output,
      map_from_entries(ARRAY[({{ field_name }}, CAST('addr_none' AS JSON))]))
    )
    {# addr_std$10 anycast:(Maybe Anycast) workchain_id:int8 address:bits256  = MsgAddressInt; #}
    WHEN {{ ton_preload_uint('state', 3) }} = 4 THEN
    ROW(
      state.has_idx, state.size, state.original_cell_data, state.cell_pointer,
      state.refs, state.exotic, state.level_,
      state.current_cell_data, 
      state.refs_indexes,
      state.cursor_bit_offset + {{ 3 + 8 + 256 }}, state.cursor_ref_offset,
      map_concat(state.output,
      map_from_entries(ARRAY[({{ field_name }}, CAST(format('%d', CAST({{ ton_preload_int(ton_skip_bits_impl(3), 8) }} AS bigint) ) || ':'
         || to_hex( CAST({{ ton_preload_uint_large(ton_skip_bits_impl(3 + 8), 256) }} as varbinary) ) AS JSON))]))
    )
    ELSE ROW(
      state.has_idx, state.size, state.original_cell_data, state.cell_pointer,
      state.refs, state.exotic, state.level_,
      state.current_cell_data, 
      state.refs_indexes,
      state.cursor_bit_offset + {{ 3 + 8 + 256 }}, state.cursor_ref_offset,
      map_concat(state.output,
      map_from_entries(ARRAY[({{ field_name }}, CAST(CAST('address format is not supported' AS BIGINT) AS JSON))]))
    )
END
AS {{ ton_boc_state_type() }})

{%- endmacro -%}