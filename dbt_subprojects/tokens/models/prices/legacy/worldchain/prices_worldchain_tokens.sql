{% set blockchain = 'worldchain' %}

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
    ('wld-worldcoin', 'WLD', 0x2cFc85d8E48F8EAB294be644d9E25C3030863003, 18)
    , ('usdc-usd-coin', 'USDC.e', 0x79A02482A880bCE3F13e09Da970dC34db4CD24d1, 6)
    , ('wbtc-wrapped-bitcoin', 'WBTC', 0x03C7054BCB39f7b2e5B2c7AcB37583e32D70Cfa3, 8)
    , ('weth-weth', 'WETH', 0x4200000000000000000000000000000000000006, 18)
    , ('sdai-savings-dai', 'sDAI', 0x859DBE24b90C9f2f7742083d3cf59cA41f55Be5d, 18)
    , ('oro-oro', 'ORO', 0xcd1E32B86953D79a6AC58e813D2EA7a1790cAb63, 18)
    , ('usdt-tether', 'USD₮0', 0x102d758f688a4c1c5a80b116bd945d4455460282, 6)
) as temp (token_id, symbol, contract_address, decimals)
