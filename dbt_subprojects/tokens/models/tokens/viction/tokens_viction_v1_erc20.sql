{{
    config(
        schema = 'tokens_viction'
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
    -- placeholder rows to give example of format, tokens missing in automated tokens.erc20
    (0xC054751BdBD24Ae713BA3Dc9Bd9434aBe2abc1ce, 'WVIC', 18)
) AS temp_table (contract_address, symbol, decimals)