{{
    config(
        schema = 'tokens_zkevm'
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
    (0x550d3bb1f77f97e4debb45d4f817d7b9f9a1affb, 'woUSDT', 6)
)
AS temp_table (contract_address, symbol, decimals)