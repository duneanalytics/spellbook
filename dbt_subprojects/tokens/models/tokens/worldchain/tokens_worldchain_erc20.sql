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
FROM 
(
    VALUES
    -- placeholder rows to give example of format, tokens already exist in tokens.erc20
    (0x2cFc85d8E48F8EAB294be644d9E25C3030863003, 'WLD', 18)
)
AS temp_table (contract_address, symbol, decimals)
