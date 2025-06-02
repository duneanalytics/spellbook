{{ config(
        schema='prices',
        alias = 'usd_native'
        )
}}
-- this is a TEMPORARY spell that should be incorporated in the general prices models.
-- more discussion here: https://github.com/duneanalytics/spellbook/issues/6577

WITH native_tokens as (
    SELECT
        name AS blockchain,
        token_symbol AS symbol,
        token_address AS contract_address,
        token_decimals AS decimals
    FROM {{ source('dune', 'blockchains') }}
    WHERE protocol = 'evm'
)

SELECT
    t.blockchain,
    t.contract_address,
    t.decimals,
    t.symbol,
    p.timestamp,
    p.price
FROM {{ source('prices', 'minute') }} p
INNER JOIN native_tokens t
ON t.blockchain = p.blockchain
and t.contract_address = p.contract_address