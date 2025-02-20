{% set blockchain = 'berachain' %}

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
    ('bera-berachain', 'WBERA', 0x6969696969696969696969696969696969696969, 18)
    , ('usdce-usd-coine', 'USDC.e', 0x549943e04f40284185054145c6E4e9568C1D3241, 6)
    , ('weth-weth', 'WETH', 0x2F6F07CDcf3588944Bf4C42aC74ff24bF56e7590, 18)
) as temp (token_id, symbol, contract_address, decimals) 