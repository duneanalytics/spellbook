{{
    config(
        schema = 'tokens_zora'
        ,alias = 'erc20'
        ,tags = ['static']
        ,materialized = 'table'
    )
}}

SELECT
    contract_address
    , symbol
    , decimals
FROM (VALUES
        (0x4200000000000000000000000000000000000006, 'WETH', 18)
        (0xa6b280b42cb0b7c4a4f789ec6ccc3a7609a1bc39, 'ENJOY', 18)
        (0x078540eecc8b6d89949c9c7d5e8e91eab64f6696, 'IMAGINE', 18)
) AS temp_table (contract_address, symbol, decimals)
