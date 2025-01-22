{{ config(
        schema='prices_scroll',
        alias = 'tokens',
        materialized='table',
        file_format = 'delta',
        tags = ['static']
        )
}}
SELECT
    token_id
    , blockchain
    , symbol
    , contract_address
    , decimals
FROM
(
    VALUES
    ('weth-weth', 'scroll', 'WETH', 0x5300000000000000000000000000000000000004, 18),
    ('wbtc-wrapped-bitcoin', 'scroll', 'WBTC', 0x3C1BCa5a656e69edCD0D4E36BEbb3FcDAcA60Cf1, 8),
    ('usdc-usd-coin', 'scroll', 'USDC', 0x06eFdBFf2a14a7c8E15944D1F4A48F9F95F663A4, 6),
    ('usdt-tether', 'scroll', 'USDT', 0xf55BEC9cafDbE8730f096Aa55dad6D22d44099Df, 6),
    ('izi-izumi-finance', 'scroll', 'iZi', 0x60D01EC2D5E98Ac51C8B4cF84DfCCE98D527c747, 18),
    ('lusd-liquity-usd', 'scroll', 'LUSD', 0xeDEAbc3A1e7D21fE835FFA6f83a710c70BB1a051, 18),
    ('wsteth-wrapped-liquid-staked-ether-20', 'scroll', 'wstETH', 0xf610A9dfB7C89644979b4A0f27063E9e7d7Cda32, 18),
    ('reth-rocket-pool-eth', 'scroll', 'rETH', 0x53878B874283351D26d206FA512aEcE1Bef6C0dD, 18),
    ('dai-dai', 'scroll', 'DAI', 0xca77eb3fefe3725dc33bccb54edefc3d9f764f97, 18),
    ('stone-stakestone-ether', 'scroll', 'STONE', 0x80137510979822322193fc997d400d5a6c747bf7, 18),
    ('sis-symbiosis-finance', 'scroll', 'SIS', 0x1467b62a6ae5cdcb10a6a8173cfe187dd2c5a136, 18),
    ('rseth-rseth', 'scroll', 'wrsETH ', 0xa25b25548b4c98b0c7d3d27dca5d5ca743d68b7f, 18),
    ('iusd-izumi-bond-usd', 'scroll', 'iUSD ', 0x0a3bb08b3a15a19b4de82f8acfc862606fb69a2d, 18),
    ('pxeth-pirex-ether', 'scroll', 'pxETH ', 0x9e0d7d79735e1c63333128149c7b616a0dc0bbdb, 18),
    ('frxeth-frax-ether', 'scroll', 'frxETH ', 0xecc68d0451e20292406967fe7c04280e5238ac7d, 18),
    ('weeth-wrapped-eeth', 'scroll', 'weETH ', 0x01f0a31698C4d065659b9bdC21B3610292a1c506, 18),
    ('solvbtc-solv-protocol-solvbtc', 'scroll', 'SolvBTC ', 0x3ba89d490ab1c0c9cc2313385b30710e838370a4, 18),
    ('pufeth-pufeth', 'scroll', 'pufETH ', 0xc4d46E8402F476F269c379677C99F18E22Ea030e, 18),
    ('usde-ethena-usde', 'scroll', 'USDe', 0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34, 18),
    ('susde-ethena-staked-usde', 'scroll', 'sUSDe', 0x211Cc4DD073734dA055fbF44a2b4667d5E5fE5d2, 18),
    ('scr-scroll', 'scroll', 'SCR', 0xd29687c813D741E2F938F4aC377128810E217b1b, 18)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
