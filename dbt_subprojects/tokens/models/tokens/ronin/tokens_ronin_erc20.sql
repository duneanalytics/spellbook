{{
    config(
        schema = 'tokens_ronin'
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
    (0xe514d9deb7966c8be0ca922de8a064264ea6bcd4, 'WRON', 18)
) AS temp_table (contract_address, symbol, decimals)