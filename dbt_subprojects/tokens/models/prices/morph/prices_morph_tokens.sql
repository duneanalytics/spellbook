{% set blockchain = 'morph' %}

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
    ('eth-ethereum', 'WETH', 0x5300000000000000000000000000000000000011, 18)
    , ('usdc-usd-coin', 'USDC', 0xCfb1186F4e93D60E60a8bDd997427D1F33bc372B, 6)
    , ('usdc-usd-coin', 'USDC.e', 0xe34c91815d7fc18A9e2148bcD4241d0a5848b693, 6)
    , ('usdt-tether', 'USDT.e', 0xc7D67A9cBB121b3b0b9c053DD9f469523243379A, 6)
    , ('usdt-tether', 'USDT0', 0xe7cd86e13ac4309349f30b3435a9d337750fc82d, 6)
    , ('usde-ethena-usde', 'USDe', 0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34, 18)
    , ('weeth-wrapped-eeth', 'weETH', 0x7dcc39b4d1c53cb31e1abc0e358b43987fef80f7, 18)
) as temp (token_id, symbol, contract_address, decimals)
