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
    , ('fsonic-fantomsonicinu', 'fSONIC', 0x05e31a691405d06708A355C029599c12d5da8b28, 18)
    , ('scusd-sonic-usd', 'scUSD', 0xd3DCe716f3eF535C5Ff8d041c1A41C3bd89b97aE, 6)
    , ('sceth-sonic-eth', 'scETH', 0x3bce5cb273f0f148010bbea2470e7b5df84c7812, 18)
    , ('sts-staked-sonic', 'stS', 0xe5da20f15420ad15de0fa650600afc998bbe3955, 18)
    , ('ag-silver', '$AG', 0x005851f943ee2957b1748957f26319e4f9edebc1, 18)
    , ('solvbtc-solv-protocol-solvbtc', 'SolvBTC', 0x541FD749419CA806a8bc7da8ac23D346f2dF8B77, 18)
    , ('solvbtcbbn-solv-protocol-solvbtcbbn', 'SolvBTC.BBN', 0xCC0966D8418d412c599A6421b760a847eB169A8c, 18)
    , ('brush-brush', 'BRUSH', 0xe51ee9868c1f0d6cd968a8b8c8376dc2991bfe44, 18)
    , ('usdc-usd-coin', 'stkscUSD', 0x4D85bA8c3918359c78Ed09581E5bc7578ba932ba, 6)
    , ('usdc-usd-coin', 'wstkscUSD', 0x9fb76f7ce5FCeAA2C42887ff441D46095E494206, 6)
    , ('sceth-sonic-eth', 'stkscETH', 0x455d5f11Fea33A8fa9D3e285930b478B6bF85265, 18)
    , ('s-sonic', 'anS', 0x0C4E186Eae8aCAA7F7de1315D5AD174BE39Ec987, 18)
    , ('s-sonic', 'OS', 0xb1e25689D55734FD3ffFc939c4C3Eb52DFf8A794, 18)
    , ('sceth-sonic-eth', 'wstkscETH', 0xE8a41c62BB4d5863C6eadC96792cFE90A1f37C47, 18)
    , ('wagmi5-wagmi', 'WAGMI', 0x0e0Ce4D450c705F8a0B6Dd9d5123e3df2787D16B, 18)
    , ('anon-heyanon', 'Anon', 0x79bbf4508b1391af3a0f4b30bb5fc4aa9ab0e07c, 18)
    , ('s-sonic', 'sS', 0x6ba47940f738175d3f8c22aa8ee8606eaae45eb2, 18)
    , ('shadow-shadow', 'SHADOW', 0x3333b97138d4b086720b5ae8a7844b1345a33333, 18)
    , ('usdt-tether', 'USDT', 0x6047828dc181963ba44974801ff68e538da5eaf9, 6)
    , ('usda-usda', 'USDa', 0xff12470a969dd362eb6595ffb44c82c959fe9acc, 18)
    , ('fbtc-ignition-fbtc', 'FBTC', 0xc96de26018a54d51c097160568752c4e3bd6c364, 8)
    , ('metro-metropolis', 'METRO', 0x71e99522ead5e21cf57f1f542dc4ad2e841f7321, 18)
    , ('derp-derp-eth', 'DERP', 0xe920d1da9a4d59126dc35996ea242d60efca1304, 18)
    , ('x33-shadow-liquid-staking-token', 'x33', 0x3333111a391cc08fa51353e9195526a70b333333, 18)
    , ('wos-wrapped-origin-sonic', 'wOS', 0x9f0df7799f6fdad409300080cff680f5a23df4b1, 18)
    , ('swpx-swapx', 'SWPx', 0xa04bc7140c26fc9bb1f36b1a604c7a5a88fb0e70, 18)
    , ('lbtc-lombard-staked-btc', 'LBTC', 0xecAc9C5F704e954931349Da37F60E39f515c11c1, 8)
    , ('wbtc-wrapped-bitcoin', 'scBTC', 0xBb30e76d9Bb2CC9631F7fC5Eb8e87B5Aff32bFbd, 8)
    , ('wbtc-wrapped-bitcoin', 'stkscBTC', 0xD0851030C94433C261B405fEcbf1DEC5E15948d0, 8)
    , ('wbtc-wrapped-bitcoin', 'wstkscBTC', 0xDb58c4DB1a0f45DDA3d2F8e44C3300BB6510c866, 8)
    , ('wbtc-wrapped-bitcoin', 'WBTC', 0x0555e30da8f98308edb960aa94c0db47230d2b9c, 8)
    , ('frxusd-frax-usd', 'frxUSD', 0x80eede496655fb9047dd39d9f418d5483ed600df, 18)
    , ('frxeth-frax-ether', 'frxETH', 0x2fb960611bdc322a9a4a994252658cae9fe2eea1, 18)
    , ('ws-wrapped-sonic', 'beS', 0x871A101Dcf22fE4fE37be7B654098c801CBA1c88, 18)
    , ('sfrxusd-staked-frax-usd', 'sfrxUSD', 0x5bff88ca1442c2496f7e475e9e7786383bc070c0, 18)
    , ('usdc-usd-coin', 'sonicUSD', 0xCb119265AA1195ea363D7A243aD56c73EA42Eb59, 18)
    , ('s-sonic', 'wmetaS', 0xbbbbbbBBbd0aE69510cE374A86749f8276647B19, 18)
    , ('s-sonic', 'metaS', 0x4444444420D9De54d69b3997b7D6A31d2BF63F32, 18)
    , ('usdc-usd-coin', 'wmetaUSD', 0xAaAaaAAac311D0572Bffb4772fe985A750E88805, 18)
    , ('usdc-usd-coin', 'metaUSD', 0x1111111199558661Bf7Ff27b4F1623dC6b91Aa3e, 18)
    , ('equal-equalizer-on-sonic', 'EQUAL', 0xddf26b42c1d903de8962d3f79a74a501420d5f19, 18)
) as temp (token_id, symbol, contract_address, decimals)
