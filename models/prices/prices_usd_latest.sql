{{ config(
        schema='prices',
        alias = alias('usd_latest'),
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
, max(pu.minute) as minute
, max_by(pu.price, pu.minute) as price
FROM {{ source('prices', 'usd') }} pu
GROUP BY 1,2,3,4
