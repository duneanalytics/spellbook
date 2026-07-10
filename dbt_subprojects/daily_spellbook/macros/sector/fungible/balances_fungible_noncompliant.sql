{% macro balances_fungible_noncompliant(transfers_agg_day, day_column='block_day') %}
WITH candidate_tokens AS (
    -- a wallet's running balance can only fall below the noncompliance threshold
    -- if the token's total negative flow does (every prefix sum >= the sum of all
    -- negative amounts); -9e14 keeps slack vs the exact -0.0010005e18 rounding
    -- boundary so float noise cannot drop a true candidate
    SELECT token_address
    FROM {{ transfers_agg_day }}
    GROUP BY token_address
    HAVING sum(if(amount < 0, amount, 0e0)) < -9e14
),

running_balances AS (
    SELECT
        t.token_address,
        SUM(t.amount) OVER (PARTITION BY t.token_address, t.wallet_address ORDER BY t.{{ day_column }}) AS amount
    FROM {{ transfers_agg_day }} t
    INNER JOIN candidate_tokens c
        ON c.token_address = t.token_address
)

SELECT DISTINCT token_address
FROM running_balances
WHERE round(amount/power(10, 18), 6) < -0.001
{% endmacro %}
