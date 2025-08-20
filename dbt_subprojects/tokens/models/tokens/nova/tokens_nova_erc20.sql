{{
    config(
        schema = 'tokens_nova'
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
    (0xf823C3cD3CeBE0a1fA952ba88Dc9EEf8e0Bf46AD, 'ARB', 18)
)
AS temp_table (contract_address, symbol, decimals)