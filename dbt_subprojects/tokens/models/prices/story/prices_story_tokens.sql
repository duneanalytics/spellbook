{% set blockchain = 'story' %}

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
    ('', 'WIP', 0x1514000000000000000000000000000000000000, 18)
    , ('usdc-usd-coin', 'USDC', 0xF1815bd50389c46847f0Bda824eC8da914045D14, 6)
    , ('', 'vIP', 0x5267F7eE069CEB3D8F1c760c215569b79d0685aD, 18)
    , ('weth-weth', 'WETH', 0xBAb93B7ad7fE8692A878B95a8e689423437cc500, 18)
) as temp (token_id, symbol, contract_address, decimals)
