{%- macro ton_boc_state_type() -%}
ROW(has_idx bigint, size bigint, original_cell_data varbinary, cell_pointer varbinary, refs bigint, exotic bigint,
level_ bigint, current_cell_data varbinary, refs_indexes varbinary, cursor_bit_offset bigint, cursor_ref_offset bigint, output map<varchar, JSON>)
{%- endmacro -%}
{%- macro ton_action_begin_parse() -%}'0'{%- endmacro -%}
{%- macro ton_action_skip_bits() -%}'1'{%- endmacro -%}
{%- macro ton_action_load_uint() -%}'2'{%- endmacro -%}
{%- macro ton_action_load_uint_large() -%}'3'{%- endmacro -%}
{%- macro ton_action_load_int() -%}'4'{%- endmacro -%}
{%- macro ton_action_load_address() -%}'5'{%- endmacro -%}
{%- macro ton_action_load_ref() -%}'6'{%- endmacro -%}
{%- macro ton_action_skip_ref() -%}'7'{%- endmacro -%}
{%- macro ton_action_restart_parse() -%}'8'{%- endmacro -%}