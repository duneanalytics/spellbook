{%- macro ton_cell_load_int(
        cell, cell_cursor, size, cast_row=true
    )
-%}
{% if size == 0 %}
{% if cast_row %}CAST({% endif %}
ROW({{ cell_cursor }}.bit_offset, {{ cell_cursor }}.ref_offset, 0){% if cast_row %} AS ROW(bit_offset bigint, ref_offset bigint, value INT256)){% endif %}
{% else %}
{% if cast_row %}CAST({% endif %}
ROW({{ cell_cursor }}.bit_offset + {{ size }}, {{ cell_cursor }}.ref_offset, IF(
    {{ ton_cell_preload_uint(cell, cell_cursor, 1) }} = 0,
    CAST({{ ton_cell_preload_uint(cell, ton_cell_skip_bits(cell_cursor, 1), size - 1) }} AS INT256),
    CAST({{ ton_cell_preload_uint(cell, ton_cell_skip_bits(cell_cursor, 1), size - 1) }} AS INT256) - bitwise_left_shift(UINT256 '1', {{ size }} - 1))
){% if cast_row %} AS ROW(bit_offset bigint, ref_offset bigint, value INT256)){% endif %}
{% endif %}

{%- endmacro -%}