{% set blockchain = 'somnia' %}

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
    ('somi-somnia', 'WSOMI', 0x046EDe9564A72571df6F5e44d0405360c0f4dCab, 18)
    , ('usdc-usd-coin', 'USDC', 0x28BEc7E30E6faee657a03e19Bf1128AaD7632A00, 6)
    , ('usdt-tether', 'USDT', 0x67b302e35aef5eee8c32d934f5856869ef428330, 6)
    , ('weth-weth', 'WETH', 0x936Ab8C674bcb567CD5dEB85D8A216494704E9D8, 18)
) as temp (token_id, symbol, contract_address, decimals)