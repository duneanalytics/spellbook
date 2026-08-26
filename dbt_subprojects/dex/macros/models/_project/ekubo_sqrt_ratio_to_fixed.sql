{#
    Converts an Ekubo `SqrtRatio` from its packed uint96 float representation to a
    Q128 fixed point value (i.e. sqrt(price) * 2^128), which is the form the Ekubo
    contracts and interface expose as `sqrtRatio`.

    Layout of the uint96 float:
        bits 95..94 : exponent (0-3)
        bits 93..0  : mantissa

        fixed = mantissa << (32 * exponent + 2)

    `sqrt_ratio_float` must be a uint256-typed expression holding the 12 raw bytes.
    Returns null when the input is null, so non-swap events pass through untouched.
#}

{% macro ekubo_sqrt_ratio_to_fixed(sqrt_ratio_float) %}
    (
        (
            {{ sqrt_ratio_float }} - CASE {{ ekubo_sqrt_ratio_exponent(sqrt_ratio_float) }}
                WHEN 0 THEN varbinary_to_uint256(0x000000000000000000000000)
                WHEN 1 THEN varbinary_to_uint256(0x400000000000000000000000)
                WHEN 2 THEN varbinary_to_uint256(0x800000000000000000000000)
                ELSE        varbinary_to_uint256(0xC00000000000000000000000)
            END
        )
        * CASE {{ ekubo_sqrt_ratio_exponent(sqrt_ratio_float) }}
            WHEN 0 THEN varbinary_to_uint256(0x04)
            WHEN 1 THEN varbinary_to_uint256(0x0400000000)
            WHEN 2 THEN varbinary_to_uint256(0x040000000000000000)
            ELSE        varbinary_to_uint256(0x04000000000000000000000000)
        END
    )
{% endmacro %}

{#
    The two most significant bits of the 96-bit float, as a bigint 0-3.
    2^94 = 19807040628566084398385987584.
#}
{% macro ekubo_sqrt_ratio_exponent(sqrt_ratio_float) %}
    cast({{ sqrt_ratio_float }} / varbinary_to_uint256(0x400000000000000000000000) as bigint)
{% endmacro %}
