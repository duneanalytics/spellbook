{{
    config(
        schema = 'tokens_sepolia'
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
    (0xb19382073c7a0addbb56ac6af1808fa49e377b75, 'BAL', 18)
    , (0x4300000000000000000000000000000000000003, 'USDB', 18)
    , (0x990c8eab51d9ecb365bf9b3de09d121af007db68, 'scUSD', 18)
    , (0xbe7a61bbc50171f3cf64e0b31323531ec3052711, 'scUSD', 18)
    , (0xaba1e60af729acd8db3a06e4305affe2ad09987c, 'scUSD', 18)
    , (0x180db257226da9ddd69f3d77fae7f39324cc4981, 'scUSD', 18)
    , (0x7b79995e5f793a07bc00c21412e50ecae098e7f9, 'WETH', 18)
) AS temp_table (contract_address, symbol, decimals)
