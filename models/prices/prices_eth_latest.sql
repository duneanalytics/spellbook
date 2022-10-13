{{ config(
        schema='prices',
        alias ='eth_latest',
        post_hook='{{ expose_spells(\'["ethereum", "solana", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c"]\',
                                    "sector",
                                    "prices",
                                    \'["msilb7"]\') }}'
        )
}}

SELECT pu.blockchain
, pu.contract_address
, pu.decimals
, pu.minute
, pu.price_eth
, pu.symbol
FROM (
    SELECT blockchain
    , contract_address
    , MAX(minute) AS latest
    FROM {{ ref('prices_eth') }}
    GROUP BY blockchain, contract_address
    ) latest
LEFT JOIN {{ ref('prices_eth') }} pu ON pu.blockchain=latest.blockchain
    AND pu.contract_address=latest.contract_address
    AND pu.minute=latest.latest

