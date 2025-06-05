{{
    config(
        schema = 'tokens_boba'
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
    (0xa18bf3994c0cc6e3b63ac420308e5383f53120d7, 'BOBA', 18)
) as temp (contract_address, symbol, decimals)
