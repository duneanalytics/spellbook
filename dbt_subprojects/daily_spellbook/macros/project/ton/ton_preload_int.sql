{%- macro ton_preload_int(
        state, size
    )
-%}
IF(
    {{ton_preload_uint(state, 1)}} = 0,
    {{ton_preload_uint(state, size)}},
    CAST({{ton_preload_uint(state, size)}} AS INT256)- bitwise_left_shift(UINT256 '1', {{ size }})
)
{%- endmacro -%}