{{ config(
        schema = 'prices_solana',
        alias = 'hour',
        materialized = 'view'
    )
}}

select
    timestamp
    , cast(year as integer) as year
    , cast(month as integer) as month
    , blockchain
    , contract_address
    , symbol
    , decimals
    , price
from
    {{ source("dune", "prices_solana_hour_raw", database="dune") }}