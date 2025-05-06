{%- macro ton_preload_uint(
        state, size
    )
-%}
bitwise_and(
    bitwise_right_shift(
        varbinary_to_uint256 (
            varbinary_substring (
                {{ state }}.current_cell_data,
                1 + cast(floor({{ state }}.cursor_bit_offset / 8e0) as bigint),
                cast(ceil({{ size }} / 8e0) as bigint) +
                IF(({{ state }}.cursor_bit_offset) % 8 = 0 OR cast(floor({{ state }}.cursor_bit_offset / 8e0) as bigint) = cast(floor(({{ state }}.cursor_bit_offset + {{ size }} - 1) / 8e0) as bigint), 0, 1) {# check that end of the slice in the next octet#}
            )
        ),
        IF(({{ size }} + {{ state }}.cursor_bit_offset) % 8 = 0, 0, 8 - ({{ size }} + {{ state}}.cursor_bit_offset) % 8)),
    bitwise_left_shift(UINT256 '1', {{ size }}) - 1
)

{%- endmacro -%}