{{
    config(
        schema = 'tokens_abstract'
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
    (0x4200000000000000000000000000000000000006, 'WETH', 18)
) AS temp_table (contract_address, symbol, decimals) 