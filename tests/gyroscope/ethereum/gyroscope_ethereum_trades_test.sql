WITH unit_test AS (

   
    SELECT

        CASE

            WHEN test.block_date = ROUND(

                actual.block_date,

                2

            ) THEN TRUE

            ELSE FALSE

        END AS block_date_test,

        CASE

            WHEN LOWER(

                test.token_bought_amount

            ) = LOWER(

                actual.token_bought_amount

            ) THEN TRUE

            ELSE FALSE

        END AS token_bought_amount_test,

        CASE

            WHEN LOWER(

                test.token_sold_amount

            ) = LOWER(

                actual.token_sold_amount

            ) THEN TRUE

            ELSE FALSE

        END AS token_sold_amount_test

    FROM

        {{ ref('gyroscope_ethereum_trades') }} AS actual

        INNER JOIN {{ ref('gyroscope_ethereum_trades_test_data') }} AS test

        ON LOWER(

            actual.tx_hash

        ) = LOWER(

            test.tx_hash

        )

)

SELECT

    *

FROM

    unit_test

WHERE

    block_date_test = FALSE

    OR token_bought_amount_test = FALSE

    OR token_sold_amount_test = FALSE
