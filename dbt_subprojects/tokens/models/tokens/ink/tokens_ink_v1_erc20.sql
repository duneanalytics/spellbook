{{
    config(
        schema = 'tokens_ink'
        ,alias = 'erc20'
        ,tags = ['static']
        ,materialized = 'table'
    )
}}

SELECT
    contract_address
    , symbol
    , decimals
FROM 
(
    VALUES
    -- placeholder rows to give example of format, tokens missing in automated tokens.erc20
    (0x4200000000000000000000000000000000000006, 'WETH', 18)
) AS temp_table (contract_address, symbol, decimals)