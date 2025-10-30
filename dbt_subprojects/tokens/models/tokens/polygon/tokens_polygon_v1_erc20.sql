{{
    config(
        schema = 'tokens_polygon'
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
    (0xe9d2fa815b95a9d087862a09079549f351dab9bd, 'BONSAI', 18)
)
AS temp_table (contract_address, symbol, decimals)