{%- macro ton_cell_load_cell(
        boc, cell_index = 0
    )
-%}

CAST(ROW(
    {# refs #}
    bitwise_and(varbinary_to_integer (varbinary_substring ({{ boc }}.cell_data, 1, 1)), 7),
    {# exotic #}
    bitwise_right_shift(bitwise_and(varbinary_to_integer (varbinary_substring ({{ boc }}.cell_data, 1, 1)), 16), 4),
    {# level #}
    bitwise_right_shift(varbinary_to_integer (varbinary_substring ({{ boc }}.cell_data, 1, 1)), 5),
    {# bits_descriptor #}
    varbinary_to_integer (varbinary_substring ({{ boc }}.cell_data, 2, 1)),
    {# cell_data #}
    varbinary_substring ({{ boc }}.cell_data, 3, varbinary_length({{ boc }}.cell_data) - 3)
    {# TODO parse refs #}

) AS ROW(refs bigint, exotic bigint, level_ bigint, bits_descriptor bigint, cell_data varbinary)) as cell,
CAST(ROW(0, 0) AS ROW(bit_offset bigint, ref_offset bigint)) as cell_cursor

{%- endmacro -%}