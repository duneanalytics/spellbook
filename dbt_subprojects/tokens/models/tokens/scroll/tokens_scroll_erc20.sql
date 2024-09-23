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
) AS temp_table (contract_address, symbol, decimals)