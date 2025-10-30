{{
    config(
        schema = 'tokens_ethereum'
        ,alias = 'erc20'
        ,tags = ['static']
        ,materialized = 'table'
    )
}}

SELECT
    contract_address as contract_address
    , trim(symbol) as symbol
    , decimals
FROM
(
    VALUES
    -- placeholder rows to give example of format, tokens already exist in tokens.erc20
    (0x01c0987e88f778df6640787226bc96354e1a9766, 'UAT', 18)
    , (0x080eb7238031f97ff011e273d6cad5ad0c2de532, 'KIT', 18)
)
AS temp_table (contract_address, symbol, decimals)