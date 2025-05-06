{{
    config(
        schema = 'tokens_shape'
        ,alias = 'erc20'
        ,tags=['static']
        ,materialized = 'table'
    )
}}

SELECT
    contract_address
    , symbol
    , decimals
FROM (VALUES
    (0xdb7DD8B00EdC5778Fe00B2408bf35C7c054f8BBe, 'USDC.e', 6)
    , (0x4200000000000000000000000000000000000006, 'WETH', 18)
    , (0x42276df82bab34c3ccca9e5c058b6ff7ea4d07e3, 'NRG', 18)
    , (0x96db3e22fdac25c0dff1cab92ae41a697406db7d, 'O', 18)
    , (0xa096a5a05c9fd4aa07a0d2b4c65a82bf12971b4b, 'SHAPEPE', 18)
    , (0x964ca3e4379420de6a095919236467bed6745502, 'T', 18)
) AS tokens(contract_address, symbol, decimals)
