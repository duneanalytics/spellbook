{{ config(
        schema='prices',
        alias ='usd_latest',
        post_hook='{{ expose_spells(\'["ethereum", "solana", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c"]\',
                                    "sector",
                                    "prices",
                                    \'["hildobby"]\') }}'
        )
}}

SELECT pu.blockchain
, pu.contract_address
, pu.decimals
, pu.minute
, pu.price
, pu.symbol
FROM (
    SELECT blockchain
    , contract_address
    , MAX(minute) AS latest
    FROM {{ source('prices', 'usd') }}
    GROUP BY blockchain, contract_address
    ) latest
LEFT JOIN {{ source('prices', 'usd') }} pu ON pu.blockchain=latest.blockchain
    AND pu.contract_address=latest.contract_address
    AND pu.minute=latest.latest

