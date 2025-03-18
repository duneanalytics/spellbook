{{ config(
        schema = 'prices',
        alias = 'minute',
        materialized = 'view'
    )
}}

select
    timestamp
    , cast(year as integer) as year
    , cast(month as integer) as month
    , blockchain
    , from_hex(contract_address) as contract_address
    , symbol
    , decimals
    , price
from
    {{ source("dune", "prices_minute_raw", database="dune") }}