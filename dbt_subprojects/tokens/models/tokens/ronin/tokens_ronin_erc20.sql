{{
    config(
        schema = 'tokens_ronin'
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
    (0xe514d9deb7966c8be0ca922de8a064264ea6bcd4, 'WRON', 18)
    , (0xc99a6a985ed2cac1ef41640596c5a5f9f4e19ef5, 'WETH', 18)
    , (0x0b7007c13325c48911f73a2dad5fa5dcbf808adc, 'USDC', 6)
    , (0x97a9107c1793bc407d6f527b77e7fff4d812bece, 'AXS', 18)
    , (0xa8754b9fa15fc18bb59458815510e40a12cd2014, 'SLP', 0)
) AS temp_table (contract_address, symbol, decimals)