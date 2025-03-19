{%- macro ton_preload_uint(
        state, size
    )
-%}
{% if size == 256 %} {# Special branch to handle 256bit since we can fit into UINT256 in case of offset % 8 != 0 in the next branch #}
IF(
    {{ state}}.cursor_bit_offset % 8 = 0,
        varbinary_to_uint256(varbinary_substring ({{ state }}.current_cell_data, 1 + {{ state }}.cursor_bit_offset / 8, {{ 256 // 8 }})),
        
        {# first byte #}
        bitwise_left_shift(
            bitwise_and(
                bitwise_left_shift(
                    varbinary_to_uint256(
                        varbinary_substring({{ state }}.current_cell_data,
                        1 + cast(floor({{ state }}.cursor_bit_offset / 8e0) as bigint),
                        1
                        )
                    ),
                    {{ state }}.cursor_bit_offset % 8),
                UINT256 '255'),
        {{ 256 - 8}})
        + 
        {# the rest #}
        bitwise_right_shift(
            varbinary_to_uint256(
                varbinary_substring({{ state }}.current_cell_data,
                1 + cast(ceil({{ state }}.cursor_bit_offset / 8e0) as bigint),
                {{ 256 // 8 }}
                )
            ),
            8 - {{ state }}.cursor_bit_offset % 8
        )
 )

{% else %}
bitwise_and(
    bitwise_right_shift(
        varbinary_to_uint256 (
            varbinary_substring (
                {{ state }}.current_cell_data,
                1 + cast(floor({{ state }}.cursor_bit_offset / 8e0) as bigint),
                cast(ceil({{ size }} / 8e0) as bigint) +
                IF(cast(floor({{ state }}.cursor_bit_offset / 8e0) as bigint) = cast(floor(({{ state }}.cursor_bit_offset + {{ size }} - 1) / 8e0) as bigint), 0, 1) {# check that end of the slice in the next octet#}
            )
        ),
        IF(({{ size }} + {{ state }}.cursor_bit_offset) % 8 = 0, 0, 8 - ({{ size }} + {{ state}}.cursor_bit_offset) % 8)),
    bitwise_left_shift(UINT256 '1', {{ size }}) - 1
)

{% endif %}

{%- endmacro -%}