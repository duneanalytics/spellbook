{% set blockchain = 'flow' %}

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
    ('weth-weth', 'WETH', 0x2F6F07CDcf3588944Bf4C42aC74ff24bF56e7590, 18)
    , ('usdc-usd-coin', 'USDC', 0xF1815bd50389c46847f0Bda824eC8da914045D14, 6)
    , ('usdce-usd-coine', 'USDC.e', 0x7f27352D5F83Db87a5A3E00f4B07Cc2138D8ee52, 6)
    , ('wflow-wrapped-flow', 'WFLOW', 0xd3bf53dac106a0290b0483ecbc89d40fcc961f3e, 18)
) as temp (token_id, symbol, contract_address, decimals)
