{{
    config(
        schema = 'tokens_abstract'
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
    (0x3439153EB7AF838Ad19d56E1571FBD09333C2809, 'WETH', 18)
    , (0x84a71ccd554cc1b02749b35d22f684cc8ec987e1, 'USDC.e', 6)
    , (0x9ebe3a824ca958e4b3da772d2065518f009cba62, 'PENGU', 18)
    , (0x000000000000000000000000000000000000800a, 'ETH', 18)
) AS temp_table (contract_address, symbol, decimals) 