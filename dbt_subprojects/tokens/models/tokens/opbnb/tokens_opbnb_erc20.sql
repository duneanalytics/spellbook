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
FROM 
(
    VALUES
    -- placeholder rows to give example of format, tokens already exist in tokens.erc20
    (0x4200000000000000000000000000000000000006, 'WBNB', 18)
) AS temp_table (contract_address, symbol, decimals)