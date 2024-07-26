{{
    config(
        schema = 'tokens_sei'
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
    (0xE30feDd158A2e3b13e9badaeABaFc5516e95e8C7, 'WSEI', 18)
    , (0x3894085Ef7Ff0f0aeDf52E2A2704928d1Ec074F1, 'USDC', 6)
    , (0xB75D0B03c06A926e488e2659DF1A861F860bD3d1, 'USDT', 6)
    , (0x160345fC359604fC6e70E3c5fAcbdE5F7A9342d8, 'WETH', 18)
) AS temp_table (contract_address, symbol, decimals)