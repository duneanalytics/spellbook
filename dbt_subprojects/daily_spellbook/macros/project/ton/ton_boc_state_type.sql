{%- macro ton_boc_state_type() -%}
ROW(has_idx bigint, size bigint, original_cell_data varbinary, cell_pointer varbinary, refs bigint, exotic bigint,
level_ bigint, current_cell_data varbinary, refs_indexes varbinary, cursor_bit_offset bigint, cursor_ref_offset bigint, output map<varchar, JSON>)
{%- endmacro -%}