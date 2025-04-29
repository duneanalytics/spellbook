{{
    config(
        schema = 'tokens_scroll'
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
    (0x60d01ec2d5e98ac51c8b4cf84dfcce98d527c747, 'iZi', 18)
    , (0xca77eb3fefe3725dc33bccb54edefc3d9f764f97, 'DAI', 18)
    , (0x6a28e90582c583fcd3347931c544819c31e9d0e0, 'BAL', 18)
    , (0xedeabc3a1e7d21fe835ffa6f83a710c70bb1a051, 'LUSD', 18)
    , (0x3c1bca5a656e69edcd0d4e36bebb3fcdaca60cf1, 'WBTC', 8)
    , (0xf55bec9cafdbe8730f096aa55dad6d22d44099df, 'USDT', 6)
    , (0x434cda25e8a2ca5d9c1c449a8cb6bcbf719233e8, 'UNI', 18)
    , (0x06efdbff2a14a7c8e15944d1f4a48f9f95f663a4, 'USDC', 6)
    , (0x53878b874283351d26d206fa512aece1bef6c0dd, 'rETH', 18)
    , (0xf610a9dfb7c89644979b4a0f27063e9e7d7cda32, 'wstETH', 18)
    , (0x608ef9a3bffe206b86c3108218003b3cfbf99c84, 'KNC', 18)
    , (0x79379c0e09a41d7978f883a56246290ee9a8c4d3, 'AAVE', 18)
    , (0x5300000000000000000000000000000000000004, 'WETH', 18)
    , (0xaaae8378809bb8815c08d3c59eb0c7d1529ad769, 'NURI', 18)
    , (0x8731d54e9d02c286767d56ac03e8037c07e01e98, 'STG', 18)
    , (0xb0643f7b3e2e2f10fe4e38728a763ec05f4adec3, 'DAPP', 18)
    , (0x1a2fcb585b327fadec91f55d45829472b15f17a4, 'TKN', 18)
    , (0x2147a89fb4608752807216d5070471c09a0dce32, 'ZP', 18)
    , (0xd29687c813d741e2f938f4ac377128810e217b1b, 'SCR', 18)
    , (0xeb466342c4d449bc9f53a865d5cb90586f405215, 'axlUSDC', 6)
    , (0x17a60bb4649a7bb885d05c008d7118a5e513d895, 'BAGGOR', 18)
    , (0x58538e6a46e07434d7e7375bc268d3cb839c0133, 'ENA', 18)
    , (0x1b896893dfc86bb67cf57767298b9073d2c1ba2c, 'CAKE', 18)
    , (0xb755039edc7910c1f1bd985d48322e55a31ac0bf, 'CRV', 18)
    , (0x087C440F251Ff6Cfe62B86DdE1bE558B95b4bb9b, 'BOLD', 18)    
) AS temp_table (contract_address, symbol, decimals)
