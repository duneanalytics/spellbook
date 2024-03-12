{{ config(
        schema = 'tokens_scroll'
        , alias = 'erc20'
        , tags=['static']
        )
}}

SELECT contract_address, symbol, decimals
FROM (VALUES
        (0x5300000000000000000000000000000000000004, 'WETH', 18)
        ,(0xf55BEC9cafDbE8730f096Aa55dad6D22d44099Df, 'USDT', 6)
        ,(0x06eFdBFf2a14a7c8E15944D1F4A48F9F95F663A4, 'USDC', 6)
        ,(0x3C1BCa5a656e69edCD0D4E36BEbb3FcDAcA60Cf1, 'WBTC', 8)
        ,(0x434cdA25E8a2CA5D9c1C449a8Cb6bCbF719233E8, 'UNI', 18)
        ,(0xcA77eB3fEFe3725Dc33bccB54eDEFc3D9f764f97, 'DAI', 18)
        ,(0x53878B874283351D26d206FA512aEcE1Bef6C0dD, 'rETH', 18)
        ,(0x79379C0E09a41d7978f883a56246290eE9a8c4d3, 'AAVE', 18)
        ,(0x6a28e90582c583fcd3347931c544819C31e9D0e0, 'BAL', 18)
        ,(0x608ef9A3BffE206B86c3108218003b3cfBf99c84, 'KNC', 18)
        ,(0xeDEAbc3A1e7D21fE835FFA6f83a710c70BB1a051, 'LUSD', 18)
        ,(0xf610A9dfB7C89644979b4A0f27063E9e7d7Cda32, 'wstETH', 18)
        ,(0x60D01EC2D5E98Ac51C8B4cF84DfCCE98D527c747, 'iZi', 18)
     ) AS temp_table (contract_address, symbol, decimals)
