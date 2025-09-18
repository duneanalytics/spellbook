{% macro
    angstrom_user_order_fill_amount(
        is_bid,
        exact_in,
        _fill_amount,
        _gas,
        _fee,
        _ray_ucp
    )
%}

WITH
    case_bools AS (
        SELECT 
            ARRAY[NOT {{ is_bid }}, {{ exact_in }}] AS cases,
            CAST({{ _fill_amount }} AS uint256) AS fill_amount,
            CAST({{ _ray_ucp }} AS uint256) AS ray_ucp,
            {{ _fee }} AS fee,
            {{ _gas }} AS gas
    ),
    amount_case AS (
        SELECT
            CASE cases
                WHEN ARRAY[true, true] THEN
                    ARRAY[
                        fill_amount,
                        floor(if(fee = 0, if(ray_ucp = 0, ray_ucp, floor(pow(10, 54) / ray_ucp)), floor((if(ray_ucp = 0, ray_ucp, floor(pow(10, 54) / ray_ucp)) * (pow(10, 6) - fee)) / pow(10, 6))) * fill_amount / pow(10, 27)) - gas,
                        floor(if(ray_ucp = 0, ray_ucp, floor(pow(10, 54) / ray_ucp)) * fill_amount / pow(10, 27)) - floor(if(fee = 0, if(ray_ucp = 0, ray_ucp, floor(pow(10, 54) / ray_ucp)), floor((if(ray_ucp = 0, ray_ucp, floor(pow(10, 54) / ray_ucp)) * (pow(10, 6) - fee)) / pow(10, 6))) * fill_amount / pow(10, 27))
                    ]
                WHEN ARRAY[true, false] THEN
                    ARRAY[
                        ceiling((fill_amount + gas) * pow(10, 27) / if(fee = 0, if(ray_ucp = 0, ray_ucp, floor(pow(10, 54) / ray_ucp)), floor((if(ray_ucp = 0, ray_ucp, floor(pow(10, 54) / ray_ucp)) * (pow(10, 6) - fee)) / pow(10, 6)))),
                        fill_amount,
                        floor(if(ray_ucp = 0, ray_ucp, floor(pow(10, 54) / ray_ucp)) * ceiling((fill_amount + gas) * pow(10, 27) / if(fee = 0, if(ray_ucp = 0, ray_ucp, floor(pow(10, 54) / ray_ucp)), floor((if(ray_ucp = 0, ray_ucp, floor(pow(10, 54) / ray_ucp)) * (pow(10, 6) - fee)) / pow(10, 6)))) / pow(10, 27)) - (fill_amount + gas)
                    ]
                WHEN ARRAY[false, true] THEN
                    ARRAY[
                        floor(if(fee = 0, ray_ucp, floor((ray_ucp * (pow(10, 6) - fee)) / pow(10, 6))) * (fill_amount - gas) / pow(10, 27)),
                        ceiling(floor(if(fee = 0, ray_ucp, floor((ray_ucp * (pow(10, 6) - fee)) / pow(10, 6))) * (fill_amount - gas) / pow(10, 27)) * pow(10, 27) / ray_ucp),
                        (fill_amount - gas) - ceiling(floor(if(fee = 0, ray_ucp, floor((ray_ucp * (pow(10, 6) - fee)) / pow(10, 6))) * (fill_amount - gas) / pow(10, 27)) * pow(10, 27) / ray_ucp)
                    ]
                WHEN ARRAY[false, false] THEN
                    ARRAY[
                        fill_amount,
                        ceiling(fill_amount * pow(10, 27) / ray_ucp),
                        (ceiling(fill_amount * pow(10, 27) / if(fee = 0, ray_ucp, floor((ray_ucp * (pow(10, 6) - fee)) / pow(10, 6)))) + gas) - gas - ceiling(fill_amount * pow(10, 27) / ray_ucp)
                    ]
            END AS cases_for_params
        FROM case_bools
    ),
    cast_cases AS (
        SELECT
            CAST(cases_for_params[2] AS uint256) AS t0_amount,
            CAST(cases_for_params[1] AS uint256) AS t1_amount,
            CAST(cases_for_params[3] AS uint256) * 75 AS lp_fees_paid_t0_unrounded,
            CAST(cases_for_params[3] AS uint256) * 25 AS protocol_fees_paid_t0_unrounded
        FROM amount_case
    )
SELECT
    t0_amount,
    t1_amount,
    if(lp_fees_paid_t0_unrounded % 100 >= protocol_fees_paid_t0_unrounded % 100, ceiling(lp_fees_paid_t0_unrounded / 100), floor(lp_fees_paid_t0_unrounded / 100)) AS lp_fees_paid_t0,
    if(lp_fees_paid_t0_unrounded % 100 >= protocol_fees_paid_t0_unrounded % 100, floor(protocol_fees_paid_t0_unrounded / 100), ceiling(protocol_fees_paid_t0_unrounded / 100)) AS protocol_fees_paid_t0
FROM cast_cases


-- TODO: investigate tiny rounding error (approx 10^-20 units off, so very insignificant)


{% endmacro %}