{{
    config(
        schema = 'tokens_shape'
        ,alias = 'erc20'
        ,tags=['static']
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
    (0xdb7DD8B00EdC5778Fe00B2408bf35C7c054f8BBe, 'USDC.e', 6)
    , (0x4200000000000000000000000000000000000006, 'WETH', 18)
) AS tokens(contract_address, symbol, decimals)