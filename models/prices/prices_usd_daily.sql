{{ config(
        schema='prices',
        alias = 'usd_daily',
        materialized = 'table',
        file_format = 'delta',
        post_hook = '{{ expose_spells(\'["ethereum", "solana", "arbitrum", "base", "gnosis", "optimism", "bnb", "avalanche_c", "polygon", "scroll", "zksync"]\',
                                    "sector",
                                    "prices",
                                    \'["aalan3"]\') }}'
        )
}}

select * from (
select
    cast(date_trunc('day', minute) as date) as day,
    blockchain,
    contract_address,
    decimals,
    symbol,
    price,
    row_number() OVER (partition by date_trunc('day', minute), blockchain, contract_address, symbol order by minute) as row_number
from {{ source('prices', 'usd') }}
) where row_number = 1

