{{
    config(
        schema = 'tokens_gnosis'
        , alias = 'erc20'
        , tags = ['static']
        , materialized = 'table'
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
    (0x385a78159a02128439cafcfdb7797ede99bf4a5f, 'YALL', 18)
    , (0x1cb903b254a13736abf7aef9e88e0b22f2c123c6, 'MEMED', 18)
)
AS temp_table (contract_address, symbol, decimals)