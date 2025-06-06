{{ config(
        schema='prices',
        alias = 'usd_native'
        )
}}
-- this is a TEMPORARY spell that should be incorporated in the general prices models.
-- more discussion here: https://github.com/duneanalytics/spellbook/issues/6577

with native_prices as (
    select
        *
    from
        {{ source('prices', 'usd') }}
    where
        blockchain is null
)

, native_tokens as (
    select
        *
    FROM {{ source('dune', 'blockchains') }}
)

select
    t.name as blockchain
    , t.token_address as contract_address
    , t.token_decimals as decimals
    , t.token_symbol as symbol
    , p.minute
    , p.price
from native_tokens as t
inner join native_prices as p
    on t.token_symbol = p.symbol