{{
    config(
        schema = 'tokens_kaia'
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
    (0x5c13e303a62fc5dedf5b52d66873f2e59fedadc2, 'USDT', 6)
)
AS temp_table (contract_address, symbol, decimals)