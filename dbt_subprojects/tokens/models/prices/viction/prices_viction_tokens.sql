{% set blockchain = 'viction' %}

{{ config(
    schema = 'prices_' + blockchain,
    alias = 'tokens',
    materialized = 'table',
    file_format = 'delta',
    tags = ['static']
    )
}}

SELECT
    token_id
    , '{{ blockchain }}' as blockchain
    , symbol
    , contract_address
    , decimals
FROM
(
    VALUES
    , ('wvic-wrapped-viction', 'WVIC', 0xC054751BdBD24Ae713BA3Dc9Bd9434aBe2abc1ce, 18)
    , ('usdt-tether', 'USDT', 0x381B31409e4D220919B2cFF012ED94d70135A59e, 6)
    , ('usdc-usd-coin', 'USDC', 0x20cC4574f263C54eb7aD630c9AC6d4d9068Cf127, 6)
) as temp (token_id, symbol, contract_address, decimals)
