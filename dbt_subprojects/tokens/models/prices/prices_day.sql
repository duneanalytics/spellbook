{{ config(
        schema = 'prices',
        alias = 'day',
        materialized = 'view',
        post_hook =
            '{{ expose_spells(\'[
                "arbitrum"
                , "avalanche_c"
                , "base"
                , "blast"
                , "bnb"
                , "celo"
                , "ethereum"
                , "fantom"
                , "gnosis"
                , "linea"
                , "mantle"
                , "nova"
                , "optimism"
                , "polygon"
                , "scroll"
                , "sei"
                , "zkevm"
                , "zksync"
                , "zora"
            ]\',
            "sector",
            "prices",
            \'["jeff-dude", "couralex"]\') }}'
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
    {{ source("dune", "prices_day_raw", database="dune") }}