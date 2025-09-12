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
    (0xbe0Ed4138121EcFC5c0E56B40517da27E6c5226B, 'ATH', 18)
)

AS temp_table (contract_address, symbol, decimals)
