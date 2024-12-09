{{
    config(
        schema = 'tokens_viction'
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
    (0xC054751BdBD24Ae713BA3Dc9Bd9434aBe2abc1ce, 'WVIC', 18)
    , (0x381B31409e4D220919B2cFF012ED94d70135A59e, 'USDT', 6)
    , (0x20cC4574f263C54eb7aD630c9AC6d4d9068Cf127, 'USDC', 6)
    , (0xb786d9c8120d311b948cf1e5aa48d8fbacf477e2, 'SAROS' 18)
    , (0x0fd0288aaae91eaf935e2ec14b23486f86516c8c, 'C98', 18)
    , (0xCdde1f5D971A369eB952192F9a5C367f33a0A891, 'SVIC', 18)
) AS temp_table (contract_address, symbol, decimals)
