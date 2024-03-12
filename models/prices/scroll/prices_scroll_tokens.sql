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
    ('reth-rocket-pool-eth', 'scroll', 'rETH', 0x53878B874283351D26d206FA512aEcE1Bef6C0dD, 18)

) as temp (token_id, blockchain, symbol, contract_address, decimals)
