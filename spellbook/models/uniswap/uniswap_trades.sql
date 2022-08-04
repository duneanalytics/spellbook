{{config
        (
                alias ='trades'
        )
}}

SELECT *
FROM
(
        SELECT *
        FROM {{ ref('uniswap_ethereum_trades') }}
)