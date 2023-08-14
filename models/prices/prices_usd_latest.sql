{{ config(
        schema='prices',
        alias = alias('usd_latest'),
        tags= ['dunesql'],
        post_hook='{{ expose_spells(\'["ethereum", "solana", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c"]\',
                                    "sector",
                                    "prices",
                                    \'["hildobby", "0xRob"]\') }}'
        )
}}

with new_prices as (
SELECT
  pu.blockchain
, pu.contract_address
, pu.decimals
, pu.symbol
, max(pu.minute) as minute
, max_by(pu.price, pu.minute) as price
FROM {{ source('prices', 'usd') }} pu
WHERE minute >= date_trunc('day', now() - interval '2' day)
GROUP BY 1,2,3,4
)
SELECT
  coalesce(new.blockchain, old.blockchain) as blockchain
, coalesce(new.contract_address, old.contract_address) as contract_address
, coalesce(new.decimals, old.decimals) as decimals
, coalesce(new.symbol, old.symbol) as symbol
, coalesce(new.minute, old.minute) as minute
, coalesce(new.price, old.price) as price
from {{ ref('prices_usd_latest_old') }} old
full outer join new_prices new
on new.blockchain = old.blockchain
and new.contract_address = old.contract_address
and new.decimals = old.decimals
and new.symbol = old.symbol
