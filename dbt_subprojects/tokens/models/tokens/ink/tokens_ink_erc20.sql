{{
    config(
        schema = 'tokens_ink'
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
    , (0xf1815bd50389c46847f0bda824ec8da914045d14, 'USDC.e', 6)
    , (0xca5f2ccbd9c40b32657df57c716de44237f80f05, 'KRAKEN', 18)
    , (0x2e1ac42aef8dc9fb4c661d017273e93ba82d3d0e, 'IAGENT', 18)
    , (0xbf0cafcbaaf0be8221ae8d630500984edc908861, 'SQUIDS', 18)
) AS temp_table (contract_address, symbol, decimals)