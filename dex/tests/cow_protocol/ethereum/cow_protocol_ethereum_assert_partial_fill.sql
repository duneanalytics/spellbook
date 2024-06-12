-- Given a list of trades, when we look at their partial_fill, based on the type of the trade we expect partial_fill to behave differently
WITH unit_test1 AS
(
    SELECT
        *
    FROM
        {{ ref('cow_protocol_ethereum_trades') }}
    WHERE
        order_uid = 0xaa1568c867c991bd462bcb6ee5939e4e01144d4e9e29bb74c2fa8d50b3afc92c519b70055af55a007110b4ff99b0ea33071c720a64289604
        and partial_fill != true
),

    unit_test2 AS
(
    SELECT
        *
    FROM
        {{ ref('cow_protocol_ethereum_trades') }}
    WHERE
        order_uid = 0xb431b648f44c8c988c417044ecdbfecf9785e177be12f600de467989284842ef40a50cf069e992aa4536211b23f286ef88752187ffffffff
        and partial_fill != false
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