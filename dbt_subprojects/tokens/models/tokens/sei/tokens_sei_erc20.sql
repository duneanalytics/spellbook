{{
    config(
        schema = 'tokens_sei'
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
    (0xE30feDd158A2e3b13e9badaeABaFc5516e95e8C7, 'WSEI', 18)
)
AS temp_table (contract_address, symbol, decimals)