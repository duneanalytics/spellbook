{{ config(
        schema='prices',
        alias ='usd_latest',
        post_hook='{{ expose_spells(\'["ethereum", "solana", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c"]\',
                                    "sector",
                                    "prices",
                                    \'["hildobby", "0xRob"]\') }}'
        )
}}

SELECT
  pu.blockchain
, pu.contract_address
, pu.decimals
, pu.symbol
, max(pu.minute)
, max_by(pu.price, pu.minute)
FROM {{ source('prices', 'usd') }} pu
WHERE minute > now() - interval 7 day
GROUP BY 1,2,3,4
