{{
    config(
        schema = 'tokens_corn'
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
    (0xda5ddd7270381a7c2717ad10d1c0ecb19e3cdfb2, 'wBTCN', 18)
) as temp (contract_address, symbol, decimals)