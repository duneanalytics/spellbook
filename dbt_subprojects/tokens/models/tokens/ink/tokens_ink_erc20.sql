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
    , (0x44c497597aa32f5F2B7eDB91522f321F04f03cD7, 'GMINKY', 18)
    , (0x1195Cf65f83B3A5768F3C496D3A05AD6412c64B7, 'CUBE', 18)
    , (0x0200C29006150606B650577BBE7B6248F58470c1, 'USDT0', 6)
    , (0xca5f2ccbd9c40b32657df57c716de44237f80f05, 'KRAKEN', 18)
    , (0x2e1ac42aef8dc9fb4c661d017273e93ba82d3d0e, 'IAGENT', 18)
    , (0xbf0cafcbaaf0be8221ae8d630500984edc908861, 'SQUIDS', 18)
) AS temp_table (contract_address, symbol, decimals)
