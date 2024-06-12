-- Given a list of solvers, when we look at the active solvers, then we should see only 1 per each env and name
WITH unit_test1 AS
(
    SELECT
        *
    FROM
        {{ ref('cow_protocol_ethereum_trades') }}
    WHERE
        order_uid = 0xc47c770fe431a2cd5fda46c84b0cdd2dbbdfb2f487e65dec444d07e7a92cffff64b07802fb794c8e7519589e85ebf67da10c9f0d640ada76
        and order_type != 'BUY'
),

    unit_test2 AS
(
    SELECT
        *
    FROM
        {{ ref('cow_protocol_ethereum_trades') }}
    WHERE
        order_uid = 0xb431b648f44c8c988c417044ecdbfecf9785e177be12f600de467989284842ef40a50cf069e992aa4536211b23f286ef88752187ffffffff
        and order_type != 'SELL'
)

SELECT
        *
FROM
    (
        SELECT
            *
        FROM
            unit_test1
        UNION
        SELECT
            *
        FROM
            unit_test2
    )