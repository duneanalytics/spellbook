{{
    config(
        schema = 'tokens_apechain'
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
    (0x48b62137edfa95a428d35c09e44256a739f6b557, 'WAPE', 18)
) AS temp_table (contract_address, symbol, decimals)