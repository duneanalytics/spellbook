{{
    config(
        schema = 'tokens_mantle'
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
    (0x19a414a6b1743315c731492cb9b7b559d7db9ab7, 'MoeLP', 18)
    , (0x1a4d4aa3bd8587f6e05cc98cf87954f7d95c11c6, 'MoeLP', 18)
)
AS temp_table (contract_address, symbol, decimals)