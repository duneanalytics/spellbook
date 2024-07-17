{% set blockchain = 'sei' %}

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
    ('sei-sei', 'WSEI', 0xE30feDd158A2e3b13e9badaeABaFc5516e95e8C7, 18)
    , ('usdc-usd-coin', 'USDC', 0x3894085Ef7Ff0f0aeDf52E2A2704928d1Ec074F1, 6)
    , ('usdt-tether', 'USDT', 0xB75D0B03c06A926e488e2659DF1A861F860bD3d1, 6)
    , ('weth-weth', 'WETH', 0x160345fC359604fC6e70E3c5fAcbdE5F7A9342d8, 18)
) as temp (token_id, symbol, contract_address, decimals)