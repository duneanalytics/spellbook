 {{ config(
       alias='trades'
       )
}}

SELECT *
FROM
(
       SELECT * FROM {{ ref('uniswap_v1_ethereum_trades') }}
       -- UNION
       -- SELECT * FROM {{ ref('uniswap_v2_ethereum_trades') }}
       -- UNION
       -- SELECT * FROM {{ ref('uniswap_v3_ethereum_trades') }}
)