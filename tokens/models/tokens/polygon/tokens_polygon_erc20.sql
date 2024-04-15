{{
    config(
        schema = 'tokens_polygon'
        ,alias = 'erc20'
        ,tags = ['static']
        ,materialized = 'table'
    )
}}

SELECT
    contract_address
    , symbol
    , decimals
FROM (VALUES
    (0x59566f90e7fc096eab985f5233c55989433d2acf, 'BAMBOO', 18)
    , (0xa03258b76ef13af716370529358f6a79eb03ec12, 'CUSDCLP', 18)
    , (0x221836a597948dce8f3568e044ff123108acc42a, 'amUSDC', 6)
    , (0xf33687811f3ad0cd6b48dd4b39f9f977bd7165a2, 'truMATIC', 18)
    , (0x1aafc31091d93c3ff003cff5d2d8f7ba2e728425, 'LP-USDC-USD+', 18)
    , (0x5e7a4558118b70e13693aea0058a0aad3193fcfd, 'TTOOLS', 18)
    , (0x5d9d8509c522a47d9285b9e4e9ec686e6a580850, 'USD+', 6)
    , (0xa63b19647787da652d0826424460d1bbf43bf9c6, 'bwAJNA', 18)
    , (0x0503dd6b2d3dd463c9bef67fb5156870af63393e, 'bb-a-DAI', 18)
    , (0xfe4db7ca9455bebb7583c764389842c85e06fe1a, 'DD', 18)
    , (0x19c60a251e525fa88cd6f3768416a8024e98fc19, 'amUSDT', 6)
    , (0x548571a302d354b190ae6e9107552ab4f7fd9dc5, 'amUSDT', 6)
    , (0x3f1f0ee83a0042a691cdf99e9f7d985da89ed6a4, 'AmeX', 8)
    , (0xef1348dac70e8349513e4ae7498f302e27102101, 'CWETHLP', 18)
    , (0x3d93f3bc2cb79c31b4df652cd332d84d16317889, 'GIT', 18)
    , (0x94d9b0506d9acecea7da59bebe1f6b59a48dbe78, 'PSFT', 18)
    , (0xee029120c72b0607344f35b17cdd90025e647b00, 'amDAI', 18)
    , (0x6deb362e86e79208f7dd60608e71596ffd88d733, '$GOLD', 6)
    , (0x9715a23d25399ef10d819e4999689de3d14eb7e2, 'AAVE', 2)
    , (0x28b36e348d6fc2160172c4e6759472e003db04a5, 'BellaFi', 18)
    , (0x8a819a4cabd6efcb4e5504fe8679a1abd831dd8f, 'bb-a-USDT', 18)
    , (0x2d6acbf1aeb0aa494cda8742c5aff697112e8bf6, 'MVAR', 18)
    , (0x5a5c6aa6164750b530b8f7658b827163b3549a4d, 'stUSD+', 6)
    , (0xa84b5b903f62ea61dfaac3f88123cc6b21bb81ab, 'amDAI', 18)
    , (0xcbfbdd24531b713e04818bcff9c7458fd72e6c82, 'BASE', 18)
    , (0xbec3a3238fac0f6a79443ade963a00456af6833e, 'USDD', 18)
    , (0x87A1fdc4C726c459f597282be639a045062c0E46, 'stataPolUSDT', 6)
    , (0x2dCa80061632f3F87c9cA28364d1d0c30cD79a19, 'stataPolUSDCn', 6)
) AS temp_table (contract_address, symbol, decimals)