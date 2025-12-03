{{
    config(
        schema = 'tokens_xlayer_v1'
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
    -- placeholder rows to give example of format
    (0x0000000000000000000000000000000000000000, 'XL', 18) -- dummy token for now
)
AS temp_table (contract_address, symbol, decimals)
