{% macro
    angstrom_user_order_fill_amount(
        is_bid,
        exact_in,
        fill_amount,
        gas,
        fee,
        ray_ucp
    )
%}

WITH
    case_bools AS (
        SELECT 
            ARRAY[is_bid, exact_in] AS cases,
            CAST(fill_amount AS uint256) AS fill_amount,
            CAST(ray_ucp AS uint256) AS ray_ucp
    ),
    amount_case AS (
        SELECT
            CASE cases
                WHEN ARRAY[true, true] THEN 
                    ARRAY[fill_amount, floor(if(fee = 0, pow(10, 54) / ray_ucp, floor(((pow(10, 54) / ray_ucp) * (pow(10, 6) - fee)) / pow(10, 6))) * fill_amount / pow(10, 27)) - gas]
                WHEN ARRAY[true, false] THEN 
                    ARRAY[ceiling((fill_amount + gas) * pow(10, 27) / if(fee = 0, pow(10, 54) / ray_ucp, floor(((pow(10, 54) / ray_ucp) * (pow(10, 6) - fee)) / pow(10, 6)))), fill_amount]
                WHEN ARRAY[false, true] THEN 
                    ARRAY[floor(if(fee = 0, ray_ucp, floor(ray_ucp * (pow(10, 6) - fee) / pow(10, 6))) * (fill_amount - gas) / pow(10, 27)), ceiling(floor(if(fee = 0, ray_ucp, floor(ray_ucp * (pow(10, 6) - fee) / pow(10, 6))) * (fill_amount - gas) / pow(10, 27)) * pow(10, 27) / ray_ucp)]
                WHEN ARRAY[false, false] THEN
                    ARRAY[fill_amount, ceiling(fill_amount * pow(10, 27) / ray_ucp)]
            END AS cases_for_params
        FROM case_bools
    )
SELECT
    CAST(cases_for_params[2] AS uint256) AS t0_amount,
    CAST(cases_for_params[1] AS uint256) AS t1_amount
FROM amount_case


{% endmacro %}