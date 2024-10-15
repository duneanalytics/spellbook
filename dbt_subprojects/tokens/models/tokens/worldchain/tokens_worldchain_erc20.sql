{{
    config(
        schema = 'tokens_worldchain'
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
    (0x2cFc85d8E48F8EAB294be644d9E25C3030863003, 'WLD', 18)
    , (0x79A02482A880bCE3F13e09Da970dC34db4CD24d1, 'USDC.e', 6)
    , (0x03C7054BCB39f7b2e5B2c7AcB37583e32D70Cfa3, 'WBTC', 8)
    , (0x4200000000000000000000000000000000000006, 'WETH', 18)
) AS temp_table (contract_address, symbol, decimals)