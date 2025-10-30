{{
    config(
        schema = 'tokens_abstract'
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
    (0x3439153EB7AF838Ad19d56E1571FBD09333C2809, 'WETH', 18)
) AS temp_table (contract_address, symbol, decimals)
