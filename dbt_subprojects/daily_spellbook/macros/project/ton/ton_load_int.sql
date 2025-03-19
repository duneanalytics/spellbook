{%- macro ton_load_int(size, field_name)
-%}
{% if size > 128 %}
  {{ exceptions.raise_compiler_error("size for ton_load_int must be less than 128") }}
{% endif %}
CAST(ROW(
    state.has_idx, state.size, state.original_cell_data, state.cell_pointer,
    state.refs, state.exotic, state.level_,
    state.current_cell_data, 
    state.refs_indexes,
    state.cursor_bit_offset + {{ size }}, state.cursor_ref_offset,
    map_concat(state.output,
    map_from_entries(ARRAY[('{{ field_name }}', CAST(CAST({{ ton_preload_int('state', size) }} AS {% if size <= 32 %}bigint{% else %}varchar{% endif %}) AS JSON))]))
) AS {{ ton_boc_state_type() }})

{%- endmacro -%}