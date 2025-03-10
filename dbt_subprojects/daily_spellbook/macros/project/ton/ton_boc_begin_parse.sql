{%- macro ton_boc_begin_parse(
        payload
    )
-%}
CASE WHEN varbinary_substring ( {{ payload }} , 1, 4) = 0xb5ee9c72 THEN
    CAST(ROW(
        {# has_idx:(## 1) has_crc32c:(## 1) has_cache_bits:(## 1) flags:(## 2) { flags = 0 } #}
        bitwise_and (varbinary_to_integer (varbinary_substring({{ payload }}, 5, 1)), 128),
        bitwise_and (varbinary_to_integer (varbinary_substring({{ payload }}, 5, 1)), 64),
        bitwise_and (varbinary_to_integer (varbinary_substring({{ payload }}, 5, 1)), 32),
        bitwise_and (varbinary_to_integer (varbinary_substring({{ payload }}, 5, 1)), 24),
        {# size:(## 3) { size <= 4 } #}
        bitwise_and (varbinary_to_integer (varbinary_substring({{ payload }}, 5, 1)), 7),
        {# off_bytes:(## 8) { off_bytes <= 8 } #} 
        varbinary_to_integer (varbinary_substring({{ payload }}, 6, 1)),
        {# cells:(##(size * 8)) #} 
        varbinary_to_integer (varbinary_substring ({{ payload }}, 7, bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7))),
        {# roots:(##(size * 8)) { roots >= 1 } #}
        varbinary_to_integer (varbinary_substring ({{ payload }}, 7 + bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7),
            bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7))),
        {# absent:(##(size * 8)) { roots + absent <= cells } #}
        varbinary_to_integer (varbinary_substring ({{ payload }}, 7 + 2 * bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7),
            bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7))),
        {# tot_cells_size:(##(off_bytes * 8)) #}
        varbinary_to_integer (varbinary_substring ({{ payload }}, 7 + 3 * bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7),
            varbinary_to_integer (varbinary_substring({{ payload }}, 6, 1)))),
        {# root_list:(roots * ##(size * 8)) - ignore it for now, assuming we always have exact one root #}
        {# index:has_idx?(cells * ##(off_bytes * 8)) #}
        CASE WHEN bitwise_and (varbinary_to_integer (varbinary_substring({{ payload }}, 5, 1)), 128) > 0
        THEN 
            varbinary_substring ({{ payload }}, 7 + 3 * bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7) {# size #}
                    + varbinary_to_integer (varbinary_substring({{ payload }}, 6, 1)) {# off_bytes #}
                    + bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7) * varbinary_to_integer (varbinary_substring ({{ payload }}, 7 + bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7),
                bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7))) {# size * roots #}
                    ,
                varbinary_to_integer (varbinary_substring ({{ payload }}, 7, bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7))) -- cells
                * 
                varbinary_to_integer (varbinary_substring({{ payload }}, 6, 1)) {# off_bytes #}
            )
        END,
        {# cell_data:(tot_cells_size * [ uint8 ]) #}
        varbinary_substring ({{ payload }}, 7 + 3 * bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7) {# size #}
                    + varbinary_to_integer (varbinary_substring({{ payload }}, 6, 1)) {# off_bytes #}
                    + bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7) * varbinary_to_integer (varbinary_substring ({{ payload }}, 7 + bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7),
                bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7))) {# size * roots #}
                + CASE WHEN bitwise_and (varbinary_to_integer (varbinary_substring({{ payload }}, 5, 1)), 128) > 0 {# has_idx? #}
                THEN
                    varbinary_to_integer (varbinary_substring ({{ payload }}, 7, bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7))) {# cells #}
                    * 
                    varbinary_to_integer (varbinary_substring({{ payload }}, 6, 1)) {# off_bytes #}
                ELSE 0
                END
                ,
            varbinary_to_integer (varbinary_substring ({{ payload }}, 7 + 3 * bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7),
                varbinary_to_integer (varbinary_substring({{ payload }}, 6, 1))))) {# tot_cells_size #}
        )
        AS ROW(has_idx bigint, has_crc32c bigint, has_cache_bits bigint, flags bigint, size bigint, off_bytes bigint,
               cells bigint, roots bigint, absent_ bigint, tot_cells_size bigint, index_ varbinary, cell_data varbinary)
    )
END

{%- endmacro -%}