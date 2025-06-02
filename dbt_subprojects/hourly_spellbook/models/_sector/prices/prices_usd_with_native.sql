{{ config(
        schema='prices',
        alias = 'usd_with_native'
        )
}}
-- this is a TEMPORARY spell that should be incorporated in the general prices models.
-- more discussion here: https://github.com/duneanalytics/spellbook/issues/6577


WITH native_token_prices AS (
    SELECT
        p.timestamp AS minute,
        b.name AS blockchain,
        b.token_address AS contract_address,
        b.token_decimals AS decimals,
        b.token_symbol AS symbol,
        p.price
    FROM {{ source('prices', 'day') }} p
    INNER JOIN {{ source('dune', 'blockchains') }} b
        ON p.blockchain = b.name
        AND p.contract_address = b.token_address
),

fallback_native_prices AS (
    SELECT
        f.minute,
        f.blockchain,
        f.contract_address,
        f.decimals,
        f.symbol,
        f.price
    FROM (
        SELECT
            minute,
            blockchain,
            contract_address,
            decimals,
            symbol,
            price
        FROM {{ source('prices', 'usd') }}
        UNION ALL
        SELECT
            minute,
            blockchain,
            contract_address,
            decimals,
            symbol,
            price
        FROM {{ ref('prices_usd_native') }}
    ) f
    LEFT JOIN (
        SELECT DISTINCT blockchain, contract_address FROM native_token_prices
    ) n
    ON f.blockchain = n.blockchain
    AND f.contract_address = n.contract_address
    WHERE n.blockchain IS NULL  
)

SELECT 
    minute,
    blockchain,
    contract_address,
    decimals,
    symbol,
    price
FROM native_token_prices

UNION ALL

SELECT 
    minute,
    blockchain,
    contract_address,
    decimals,
    symbol,
    price
FROM fallback_native_prices