{%- macro ton_cell_preload_uint(
        cell, cell_cursor, size
    )
-%}
{% if size == 256 %} {# Special branch to handle 256bit since we can fit into UINT256 in case of offset % 8 != 0 in the next branch #}
IF(
    {{ cell_cursor}}.bit_offset % 8 = 0,
        varbinary_to_uint256(varbinary_substring ({{ cell }}.cell_data,1 + {{ cell_cursor }}.bit_offset / 8, {{ 256 // 8 }})),
        
        {# first byte #}
        bitwise_left_shift(
            bitwise_and(
                bitwise_left_shift(
                    varbinary_to_uint256(
                        varbinary_substring({{ cell }}.cell_data,
                        1 + cast(floor({{ cell_cursor }}.bit_offset / 8e0) as bigint),
                        1
                        )
                    ),
                    {{ cell_cursor }}.bit_offset % 8),
                UINT256 '255'),
        {{ 256 - 8}})
        + 
        {# the rest #}
        bitwise_right_shift(
            varbinary_to_uint256(
                varbinary_substring({{ cell }}.cell_data,
                1 + cast(ceil({{ cell_cursor }}.bit_offset / 8e0) as bigint),
                {{ 256 // 8 }}
                )
            ),
            8 - {{ cell_cursor }}.bit_offset % 8
        )
 )

{% else %}
bitwise_and(
    bitwise_right_shift(
        varbinary_to_uint256 (
            varbinary_substring (
                {{ cell }}.cell_data,
                1 + cast(floor({{ cell_cursor }}.bit_offset / 8e0) as bigint), {# + 1 + CASE WHEN {{ cell }}.exotic > 0 THEN ({{ cell }}.level_ + 1) * 34 ELSE 0 END,#}
                cast(ceil({{ size }} / 8e0) as bigint) + IF({{ cell_cursor }}.bit_offset % 8 = 0, 0, 1)
            )
        ),
        IF(({{ size }} + {{ cell_cursor}}.bit_offset) % 8 = 0, 0, 8 - ({{ size }} + {{ cell_cursor}}.bit_offset) % 8)),
    bitwise_left_shift(UINT256 '1', {{ size }}) - 1
)

{% endif %}

{%- endmacro -%}