{{ config(
        schema='prices',
        alias = 'usd_daily',
        materialized = 'table'

        post_hook='{{ expose_spells(\'["ethereum", "solana", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c", "zksync"]\',
                                    "sector",
                                    "prices",
                                    \'["aalan3"]\') }}'
        )
}}

SELECT
blockchain,
contract_address,
decimals,
symbol,
date_trunc('day', minute) as day,
min_by(price, minute) as price, -- first price of the day
from {{ source('prices', 'usd') }}
group by 1, 2, 3, 4, 5
