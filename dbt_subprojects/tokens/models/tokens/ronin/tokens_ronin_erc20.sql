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
    , (0x18d2bdef572c67127e218c425f546fe64430a92c, 'LUAUSD', 18)
    , (0x7eae20d11ef8c779433eb24503def900b9d28ad7, 'PIXEL', 18)
    , (0xd61bbbb8369c46c15868ad9263a2710aced156c4, 'LUA', 18)
    , (0x7894b3088d069e70895effa4e8f7d2c243fd04c1, 'APRS', 18)
    , (0xf80132fc0a86add011bffce3aedd60a86e3d704d, 'ANIMA', 18)
) AS temp_table (contract_address, symbol, decimals)
