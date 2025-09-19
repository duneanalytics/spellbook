{% macro
    angstrom_composable_protocol_fee_calc(
        _fee_in_e6,
        _zfo,
        _amount_specified,
        _swapped_amount0,
        _swapped_amount1
    )
%}



WITH
    const_params AS (
        SELECT
            {{ _amount_specified }} < 0 AS exact_in,
            {{ _swapped_amount0 }} AS swapped_amount0,
            {{ _swapped_amount1 }} AS swapped_amount1,
            {{ _zfo }} AS zfo,
            {{ _fee_in_e6 }} AS fee_in_e6
    ),
    merged_const_params AS (
        SELECT
            exact_in,
            fee_in_e6,
            ABS(if(exact_in != zfo, swapped_amount0, swapped_amount1)) AS amount
        FROM const_params
    )
SELECT
    ROUND(if(exact_in, amount * fee_in_e6 / 1e6, amount * 1e6 / (1e6 - fee_in_e6) - amount)) AS fee_amount
FROM merged_const_params

 

{% endmacro %}