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
    , ('wagmi5-wagmi', 'WAGMI', 0x0e0Ce4D450c705F8a0B6Dd9d5123e3df2787D16B, 18)
    , ('anon-heyanon', 'Anon', 0x79bbf4508b1391af3a0f4b30bb5fc4aa9ab0e07c, 18)
    , ('shadow-shadow', 'SHADOW', 0x3333b97138d4b086720b5ae8a7844b1345a33333, 18)
) as temp (token_id, symbol, contract_address, decimals)
