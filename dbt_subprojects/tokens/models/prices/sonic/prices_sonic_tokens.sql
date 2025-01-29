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
    , ('ag-silver', 'AG', 0x005851f943ee2957b1748957f26319e4f9edebc1, 18)
    , ('solvbtc-solv-protocol-solvbtc', 'SOLVBTC', 0x541FD749419CA806a8bc7da8ac23D346f2dF8B77, 18)
    , ('solvbtcbbn-solv-protocol-solvbtcbbn', 'SOLVBTCBBN', 0xCC0966D8418d412c599A6421b760a847eB169A8c, 18)
    , ('brush-brush', 'BRUSH', 0xe51ee9868c1f0d6cd968a8b8c8376dc2991bfe44, 18)
    , ('stkscusd-staked-sonic-usd', 'stkscUSD', 0x4D85bA8c3918359c78Ed09581E5bc7578ba932ba, 6)
    , ('wstkscusd-wrapped-staked-sonic-usd', 'wstkscUSD', 0x9fb76f7ce5FCeAA2C42887ff441D46095E494206, 6)
    , ('stksceth-staked-sonic-eth', 'stkscETH', 0x455d5f11Fea33A8fa9D3e285930b478B6bF85265, 18)
    , ('ans-angles-staked-sonic', 'anS', 0x0C4E186Eae8aCAA7F7de1315D5AD174BE39Ec987, 18)
    , ('wans-wrapped-angles-sonic', 'wanS', 0xfA85Fe5A8F5560e9039C04f2b0a90dE1415aBD70, 18)
    , ('os-origin-sonic', 'OS', 0xb1e25689D55734FD3ffFc939c4C3Eb52DFf8A794, 18)
    , ('wos-wrapped-origin-sonic', 'wOS', 0x9F0dF7799f6FDAd409300080cfF680f5A23df4b1, 18)
    , ('wstksceth-wrapped-staked-sonic-eth', 'wstkscETH', 0xE8a41c62BB4d5863C6eadC96792cFE90A1f37C47, 18)
) as temp (token_id, symbol, contract_address, decimals)
