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
) AS temp_table (contract_address, symbol, decimals)