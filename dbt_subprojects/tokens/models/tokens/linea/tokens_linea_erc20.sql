{{
    config(
        schema = 'tokens_linea'
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
    (0xe5d7c2a44ffddf6b295a15c148167daaaf5cf34f, 'WETH', 18),
    (0xaaaac83751090c6ea42379626435f805ddf54dc8, 'NILE', 18),
    (0x78354f8dccb269a615a7e0a24f9b0718fdc3c7a7, 'ZERO', 18),
    (0x1bf74c010e6320bab11e2e5a532b5ac15e0b8aa6, 'weETH', 18),
    (0xa219439258ca9da29e9cc4ce5596924745e12b93, 'USDT', 6),
    (0x5fbdf89403270a1846f5ae7d113a989f850d1566, 'FOXY', 18),
    (0xd2671165570f41bbb3b0097893300b6eb6101e6c, 'wrsETH', 18),
    (0x43e8809ea748eff3204ee01f08872f063e44065f, 'MENDI', 18),
    (0x176211869ca2b568f2a7d4ee941e073a821ee1ff, 'USDC', 6),
    (0x63ba74893621d3d12f13cec1e86517ec3d329837, 'LUSD', 18),
    (0xacb54d07ca167934f57f829bee2cc665e1a5ebef, 'CROAK', 18),
    (0x82cc61354d78b846016b559e3ccd766fa7e793d5, 'LINDA', 18),
    (0x2416092f143378750bb29b79ed961ab195cceea5, 'ezETH', 18),
    (0x3aab2285ddcddad8edf438c1bab47e1a9d05a9b4, 'WBTC', 8),
    (0x4af15ec2a0bd43db75dd04e62faa3b8ef36b00d5, 'DAI', 18),
    (0x894134a25a5fac1c2c26f1d8fbf05111a3cb9487, 'GRAI', 18),
    (0xeb466342c4d449bc9f53a865d5cb90586f405215, 'axlUSDC', 6),
    (0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34, 'USDe', 18),
    (0x59846bfc18fc21df6bed378748f99ea38f44d50a, 'lzIT', 6),
    (0x1a51b19ce03dbe0cb44c1528e34a7edd7771e9af, 'LYNX', 18),
    (0x2442bd7ae83b51f6664de408a385375fe4a84f52, 'MKR', 18),
    (0x58538e6a46e07434d7e7375bc268d3cb839c0133, 'ENA', 18),
    (0x7da14988e4f390c2e34ed41df1814467d3ade0c3, 'PEPE', 18),
    (0x99ad925c1dc14ac7cc6ca1244eef8043c74e99d5, 'SHIB', 18),
    (0x4186bfc76e2e237523cbc30fd220fe055156b41f, 'rsETH', 18)
) AS temp_table (contract_address, symbol, decimals)
