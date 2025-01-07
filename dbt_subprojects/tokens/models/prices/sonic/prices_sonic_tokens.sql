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
    ('ws-wrapped-sonic', 'wS', 0x039e2fB66102314Ce7b64Ce5Ce3E5183bc94aD38, 18)
    , ('usdc-usd-coin', 'USDC.e', 0x29219dd400f2Bf60E5a23d13Be72B486D4038894, 6)
    , ('weth-weth', 'WETH', 0x50c42dEAcD8Fc9773493ED674b675bE577f2634b, 18)
    , ('euroc-euro-coin', 'EURC.e', 0xe715cba7b5ccb33790cebff1436809d36cb17e57, 6)
    , ('beets-beethoven-x', 'BEETS', 0x2d0e0814e62d80056181f5cd932274405966e4f0, 18)
    , ('goglz-goggles', 'GOGLZ', 0x9fDbC3f8Abc05Fa8f3Ad3C17D2F806c1230c4564, 18)
    , ('fsonic-fantomsonicinu', 'FSONIC', 0x05e31a691405d06708A355C029599c12d5da8b28, 18)
    , ('scusd-sonic-usd', 'scUSD', 0xd3DCe716f3eF535C5Ff8d041c1A41C3bd89b97aE, 6)
    , ('sceth-sonic-eth', 'scETH', 0x3bce5cb273f0f148010bbea2470e7b5df84c7812, 18)
    , ('sts-staked-sonic', 'stS', 0xe5da20f15420ad15de0fa650600afc998bbe3955, 18)
) as temp (token_id, symbol, contract_address, decimals)
