{{ config(
        schema='prices',
        alias = alias('usd_latest'),
        tags= ['dunesql'],
        post_hook='{{ expose_spells(\'["ethereum", "solana", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c", "zksync"]\',
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
from {{ ref('prices_usd_latest_historical') }} old
full outer join new_prices new
on (new.blockchain = old.blockchain OR (new.blockchain is null and old.blockchain is null))
and (new.contract_address = old.contract_address OR (new.contract_address is null and old.contract_address is null))
and (new.decimals = old.decimals OR (new.decimals is null and old.decimals is null))
and (new.symbol = old.symbol OR (new.symbol is null and old.symbol is null))
