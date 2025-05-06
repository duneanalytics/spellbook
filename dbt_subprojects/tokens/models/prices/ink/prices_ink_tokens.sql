{% set blockchain = 'ink' %}

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
    ('weth-weth', 'WETH', 0x4200000000000000000000000000000000000006, 18)
    , ('usdc-usd-coin', 'USDC.e', 0xF1815bd50389c46847f0Bda824eC8da914045D14, 6)
    , ('krill-krill', 'KRILL', 0xcb95a3840c8ea5f0d4e78b67ec897df84d17c5e6, 18)
    , ('purple-purple-coin', 'PURPLE', 0xd642b49d10cc6e1bc1c6945725667c35e0875f22, 18)
    , ('usdt-tether', 'USD₮0', 0x0200C29006150606B650577BBE7B6248F58470c1, 6)
) as temp (token_id, symbol, contract_address, decimals)