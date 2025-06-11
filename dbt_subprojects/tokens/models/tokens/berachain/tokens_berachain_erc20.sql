{{
    config(
        schema = 'tokens_berachain'
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
    (0x6969696969696969696969696969696969696969, 'WBERA', 18)
) AS temp_table (contract_address, symbol, decimals)