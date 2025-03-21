{%- macro ton_from_boc(payload, actions) -%}
REDUCE(
ARRAY[
    {#
    Action layout: [action op code, output value name, arguments]
    #}
    {% for action in actions %}
        {{ action }}{% if not loop.last %},{% endif %}
    {% endfor %}
],
CAST(
    CASE WHEN varbinary_substring ( {{ payload }} , 1, 4) = 0xb5ee9c72 THEN
        CAST(ROW(
            {# has_idx:(## 1) has_crc32c:(## 1) has_cache_bits:(## 1) flags:(## 2) { flags = 0 } #}
            bitwise_and (varbinary_to_integer (varbinary_substring({{ payload }}, 5, 1)), 128),
            {#
            -- bitwise_and (varbinary_to_integer (varbinary_substring({{ payload }}, 5, 1)), 64),
            --bitwise_and (varbinary_to_integer (varbinary_substring({{ payload }}, 5, 1)), 32),
            --bitwise_and (varbinary_to_integer (varbinary_substring({{ payload }}, 5, 1)), 24)
            
            size:(## 3) { size <= 4 } #}
            bitwise_and (varbinary_to_integer (varbinary_substring({{ payload }}, 5, 1)), 7),
            {# off_bytes:(## 8) { off_bytes <= 8 }
            -- varbinary_to_integer (varbinary_substring({{ payload }}, 6, 1)),
            cells:(##(size * 8))
            -- varbinary_to_integer (varbinary_substring ({{ payload }}, 7, bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7))),
            roots:(##(size * 8)) { roots >= 1 } 
            --varbinary_to_integer (varbinary_substring ({{ payload }}, 7 + bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7),
                --bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7))),
            absent:(##(size * 8)) { roots + absent <= cells }
            --varbinary_to_integer (varbinary_substring ({{ payload }}, 7 + 2 * bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7),
                --bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7))),
            tot_cells_size:(##(off_bytes * 8))
            --varbinary_to_integer (varbinary_substring ({{ payload }}, 7 + 3 * bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7),
                --varbinary_to_integer (varbinary_substring({{ payload }}, 6, 1)))),
             root_list:(roots * ##(size * 8)) - ignore it for now, assuming we always have exact one root 
             index:has_idx?(cells * ##(off_bytes * 8)) 
            -- CASE WHEN bitwise_and (varbinary_to_integer (varbinary_substring({{ payload }}, 5, 1)), 128) > 0
            --THEN 
            --    varbinary_substring ({{ payload }}, 7 + 3 * bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7)
                        --+ varbinary_to_integer (varbinary_substring({{ payload }}, 6, 1))
                        --+ bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7) * varbinary_to_integer (varbinary_substring ({{ payload }}, 7 + bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7),
                    --bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7)))
                        --,
                --  varbinary_to_integer (varbinary_substring ({{ payload }}, 7, bitwise_and(varbinary_to_integer(varbinary_substring({{ payload }}, 5, 1)), 7))) -- cells
                    --* 
                    --varbinary_to_integer (varbinary_substring({{ payload }}, 6, 1))
                --)
            --END,
            cell_data:(tot_cells_size * [ uint8 ]) #}
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
                    varbinary_to_integer (varbinary_substring({{ payload }}, 6, 1))))), {# tot_cells_size #}
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
                    varbinary_to_integer (varbinary_substring({{ payload }}, 6, 1))))), {# tot_cells_size #}
            0 {# refs #}, 0 {# exotic #}, 0 {# level_ #}, null {# current_cell_data #}, null {# refs_indexes #}, 0 {# cursor_bit_offset #}, 0 {# cursor_ref_offset #},
            map() {# output #}
            )
            AS {{ ton_boc_state_type() }}
        )
    END
AS {{ ton_boc_state_type() }})
,
(state, step) -> CASE
    {# layout:
         [1]                  [2]               [3]           [4][5]...
    [action op code, output value name, output value type, arguments...]
    #}
    WHEN step[1] = {{ ton_action_begin_parse() }} THEN {{ ton_begin_parse_impl() }}

    WHEN step[1] = {{ ton_action_skip_bits() }} THEN {{ ton_skip_bits_impl('CAST(step[4] AS bigint)') }}
    WHEN step[1] = {{ ton_action_load_uint() }} THEN {{ ton_load_uint_impl('CAST(step[4] AS bigint)', 'step[2]') }}
    WHEN step[1] = {{ ton_action_load_uint_large() }} THEN {{ ton_load_uint_large_impl('CAST(step[4] AS bigint)', 'step[2]') }}
    WHEN step[1] = {{ ton_action_load_int() }} THEN {{ ton_load_int_impl('CAST(step[4] AS bigint)', 'step[2]') }}
    WHEN step[1] = {{ ton_action_load_address() }} THEN {{ ton_load_address_impl('step[2]') }}
    WHEN step[1] = {{ ton_action_load_ref() }} THEN {{ ton_load_ref_impl() }}
    WHEN step[1] = {{ ton_action_skip_ref() }} THEN {{ ton_skip_refs_impl('CAST(step[4] AS bigint)') }}
    WHEN step[1] = {{ ton_action_restart_parse() }} THEN {{ ton_restart_parse_impl() }}
END,
s -> CAST(ROW(
    {#- prepare list of fields to be returned -#}
    {%- set fields = [] -%}
    {%- for action in actions -%}
        {%- set output_field_name = action.strip()[6:-1].split(',')[1].strip() -%}
        {%- if output_field_name != 'null' -%}
            {%- if output_field_name in fields -%}
                {{ exceptions.raise_compiler_error("Field " + output_field_name + " is duplicated") }}
            {%- endif %}
            {%- set fields = fields.append(output_field_name) -%}
        {%- endif -%}
    {%- endfor %}
    {%- set counter = namespace(value=0) -%}
    {%- for action in actions -%}
        {% set output_field_name = action.strip()[6:-1].split(',')[1].strip() %}
        {% if output_field_name in fields -%}
            {%- set output_field_type = action.strip()[6:-1].split(',')[2].strip() -%}
            {%- set counter.value = counter.value + 1 -%}
            {% if output_field_type | replace("'", "") == 'UINT256' or output_field_type | replace("'", "") == 'INT256' %}
                CAST(CAST(s.output[{{ output_field_name }}] AS varchar) AS {{ output_field_type | replace("'", "") }}) {% if counter.value < fields | length %},{% endif %}
            {% else %}
                CAST(s.output[{{ output_field_name }}] AS {{ output_field_type | replace("'", "") }}) {% if counter.value < fields | length %},{% endif %}
            {% endif %}
        {%- endif %}
    {%- endfor %}
) AS ROW(
    {%- set counter = namespace(value=0) -%}
    {% for action in actions -%}
        {%- set output_field_name = action.strip()[6:-1].split(',')[1].strip()  -%}
        {%- if output_field_name in fields -%}
            {%- set output_field_type = action.strip()[6:-1].split(',')[2].strip() -%}
            {%- set counter.value = counter.value + 1 -%}
            {{ output_field_name | replace("'", "")}} {{ output_field_type | replace("'", "") }} {% if counter.value < fields | length %},{% endif %}
        {%- endif %}
    {%- endfor %}))
)
{%- endmacro -%}