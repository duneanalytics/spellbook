{% set blockchain = 'sonic' %}

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
    ('ftm-fantom', 'wS', 0x039e2fB66102314Ce7b64Ce5Ce3E5183bc94aD38, 18) -- looks like FTM <--> S token are same price and 1:1 upgrade according to docs
    , ('usdc-usd-coin', 'USDC.e', 0x29219dd400f2Bf60E5a23d13Be72B486D4038894, 6)
    , ('weth-weth', 'WETH', 0x50c42dEAcD8Fc9773493ED674b675bE577f2634b, 18)
    , ('euroc-euro-coin', 'EURC.e', 0xe715cba7b5ccb33790cebff1436809d36cb17e57, 6)
    , ('beets-beethoven-x', 'BEETS', 0x2d0e0814e62d80056181f5cd932274405966e4f0, 18)
) as temp (token_id, symbol, contract_address, decimals)
