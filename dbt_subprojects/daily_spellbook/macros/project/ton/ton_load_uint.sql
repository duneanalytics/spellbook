{%- macro ton_load_uint(size, field_name)
-%}
{% if size > 256 %}
  {{ exceptions.raise_compiler_error("size for ton_load_uint must be less than 256") }}
{% endif %}
CAST(ROW(
    state.has_idx, state.size, state.original_cell_data, state.cell_pointer,
    state.refs, state.exotic, state.level_,
    state.current_cell_data, 
    state.refs_indexes,
    state.cursor_bit_offset + {{ size }}, state.cursor_ref_offset,
    map_concat(state.output,
    map_from_entries(ARRAY[('{{ field_name }}', CAST(CAST({{ ton_preload_uint('state', size) }} AS {% if size <= 64 %}bigint{% else %}varchar{% endif %}) AS JSON))]))
) AS {{ ton_boc_state_type() }})

{%- endmacro -%}