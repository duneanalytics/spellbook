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
FROM 
(
    VALUES
    -- placeholder rows to give example of format, tokens already exist in tokens.erc20
    (0xe5d7c2a44ffddf6b295a15c148167daaaf5cf34f, 'WETH', 18)
)
AS temp_table (contract_address, symbol, decimals)