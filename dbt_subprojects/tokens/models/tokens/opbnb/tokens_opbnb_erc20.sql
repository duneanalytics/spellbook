{{
    config(
        schema = 'tokens_opbnb'
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
    (0x4200000000000000000000000000000000000006, 'WBNB', 18)
    , (0x9e5aac1ba1a2e6aed6b32689dfcf62a509ca96f3, 'USDT', 18)
    , (0x0000000000000000000000000000000000000000, 'BNB', 18)
) AS temp_table (contract_address, symbol, decimals)
