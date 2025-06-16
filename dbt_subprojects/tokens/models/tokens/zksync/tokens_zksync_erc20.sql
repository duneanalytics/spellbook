{{
    config(
        schema = 'tokens_zksync'
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
    (0x3f0b8b206a7fbdb3ecfc08c9407ca83f5ab1ce59, '1INCH', 18)
)
AS temp_table (contract_address, symbol, decimals)