{{
    config(
        schema = 'tokens_plume'
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
    (0xEa237441c92CAe6FC17Caaf9a7acB3f953be4bd1, 'WPLUME', 18)
) AS temp_table (contract_address, symbol, decimals)