{%- macro ton_get_ref_cell()
-%}
{# Returns ref_offset for the given ref_index #}
reduce(
    sequence(1, CAST(varbinary_to_uint256(
                varbinary_substring(
                    state.refs_indexes,
                    1 + state.cursor_ref_offset * state.size,
                    state.size
                )
            ) AS bigint)),
    state.original_cell_data,
    (l, x) -> varbinary_substring(l, 1 + 2 + bitwise_right_shift(varbinary_to_integer (varbinary_substring (l, 2, 1)), 1) + 
    bitwise_and(varbinary_to_integer (varbinary_substring (l, 2, 1)), 1) + 
    bitwise_and(varbinary_to_integer (varbinary_substring (l, 1, 1)), 7) * state.size ),
    s->s
)
{%- endmacro -%}